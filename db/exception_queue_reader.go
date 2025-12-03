package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// ExceptionQueueReader инкапсулирует работу с exception queue Oracle AQ
// Exception queue используется для хранения ошибочных сообщений (state = 3)
type ExceptionQueueReader struct {
	dbConn      *DBConnection
	queueName   string
	waitTimeout int // в секундах
	mu          sync.Mutex
}

// NewExceptionQueueReader создает новый экземпляр ExceptionQueueReader
func NewExceptionQueueReader(dbConn *DBConnection) (*ExceptionQueueReader, error) {
	cfg := dbConn.cfg
	if cfg == nil {
		return nil, errors.New("конфигурация не загружена")
	}

	queueSec := cfg.Section("queue")
	exceptionQueueName := queueSec.Key("exception_queue_name").String()
	if exceptionQueueName == "" {
		return nil, errors.New("exception_queue_name не указан в конфигурации")
	}

	return &ExceptionQueueReader{
		dbConn:      dbConn,
		queueName:   exceptionQueueName,
		waitTimeout: 2, // 2 секунды по умолчанию
	}, nil
}

// DequeueMany извлекает несколько сообщений из exception queue
// Возвращает слайс сообщений, может быть пустым если очередь пуста
// Принимает контекст для возможности отмены операций при graceful shutdown
func (eqr *ExceptionQueueReader) DequeueMany(ctx context.Context, count int) ([]*QueueMessage, error) {
	// Проверяем соединение БЕЗ блокировки мьютекса (быстрая проверка)
	if eqr.dbConn.db == nil {
		return nil, errors.New("соединение с БД не открыто")
	}

	if count <= 0 {
		count = 1
	}

	// Читаем конфигурацию под блокировкой (только для чтения)
	eqr.mu.Lock()
	queueName := eqr.queueName
	waitTimeout := eqr.waitTimeout
	eqr.mu.Unlock()

	var messages []*QueueMessage

	// Извлекаем сообщения по одному
	for i := 0; i < count; i++ {
		// Проверяем контекст перед каждой итерацией для возможности прерывания
		select {
		case <-ctx.Done():
			if logger.Log != nil {
				logger.Log.Info("Операция чтения из exception queue прервана",
					zap.Int("received", len(messages)))
			}
			return messages, ctx.Err()
		default:
		}

		msg, err := eqr.dequeueOneMessage(ctx, queueName, waitTimeout)
		if err != nil {
			// Если ошибка связана с отменой контекста, возвращаем частичный результат
			if ctx.Err() != nil {
				if logger.Log != nil {
					logger.Log.Info("Операция чтения из exception queue прервана из-за отмены контекста",
						zap.Int("received", len(messages)))
				}
				return messages, ctx.Err()
			}
			return messages, err
		}
		if msg == nil {
			// Очередь пуста
			break
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// dequeueOneMessage извлекает одно сообщение из exception queue
// Использует DBMS_AQ.DEQUEUE с XMLType payload
// НЕ использует consumer_name для exception queue
// Принимает контекст для возможности отмены операций при graceful shutdown
// Принимает queueName и waitTimeout как параметры, чтобы не блокировать мьютекс на весь метод
func (eqr *ExceptionQueueReader) dequeueOneMessage(ctx context.Context, queueName string, waitTimeout int) (*QueueMessage, error) {
	if logger.Log != nil {
		logger.Log.Debug("Попытка извлечения сообщения из exception queue",
			zap.String("queue", queueName),
			zap.Int("timeout", waitTimeout))
	}

	// Создаем контекст с таймаутом для создания пакета (объединяем с переданным контекстом)
	packageCtx, packageCancel := context.WithTimeout(ctx, execTimeout)
	defer packageCancel()

	// Создаем пакет с переменными и функциями для доступа к ним
	// Используем отдельный пакет для exception queue, чтобы не конфликтовать с обычной очередью
	createPackageSQL := `
		CREATE OR REPLACE PACKAGE temp_exception_queue_pkg AS
			g_msgid RAW(16);
			g_payload XMLType;
			g_success NUMBER := 0;
			g_error_code NUMBER := 0;
			g_error_msg VARCHAR2(4000);
			
			FUNCTION get_success RETURN NUMBER;
			FUNCTION get_error_code RETURN NUMBER;
			FUNCTION get_error_msg RETURN VARCHAR2;
			FUNCTION get_msgid RETURN RAW;
			FUNCTION get_payload RETURN XMLType;
		END temp_exception_queue_pkg;
	`
	_, err := eqr.dbConn.db.ExecContext(packageCtx, createPackageSQL)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Debug("Не удалось создать пакет (возможно, уже существует)", zap.Error(err))
		}
	}

	// Создаем тело пакета с реализацией функций
	createPackageBodySQL := `
		CREATE OR REPLACE PACKAGE BODY temp_exception_queue_pkg AS
			FUNCTION get_success RETURN NUMBER IS
			BEGIN
				RETURN g_success;
			END;
			
			FUNCTION get_error_code RETURN NUMBER IS
			BEGIN
				RETURN g_error_code;
			END;
			
			FUNCTION get_error_msg RETURN VARCHAR2 IS
			BEGIN
				RETURN g_error_msg;
			END;
			
			FUNCTION get_msgid RETURN RAW IS
			BEGIN
				RETURN g_msgid;
			END;
			
			FUNCTION get_payload RETURN XMLType IS
			BEGIN
				RETURN g_payload;
			END;
		END temp_exception_queue_pkg;
	`
	_, err = eqr.dbConn.db.ExecContext(packageCtx, createPackageBodySQL)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Debug("Не удалось создать тело пакета (возможно, уже существует)", zap.Error(err))
		}
	}

	// PL/SQL блок для выполнения dequeue из exception queue
	// ВАЖНО: для exception queue НЕ указываем consumer_name
	plsql := `
		DECLARE
			v_dequeue_options DBMS_AQ.dequeue_options_t;
			v_message_properties DBMS_AQ.message_properties_t;
		BEGIN
			-- Инициализируем переменные пакета
			temp_exception_queue_pkg.g_success := 0;
			temp_exception_queue_pkg.g_error_code := 0;
			temp_exception_queue_pkg.g_error_msg := NULL;
			
			-- Настраиваем опции dequeue
			-- Для exception queue НЕ указываем consumer_name
			v_dequeue_options.dequeue_mode := DBMS_AQ.REMOVE;
			v_dequeue_options.wait := :1;
			v_dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
			
			-- Выполняем dequeue и сохраняем результат в пакетные переменные
			DBMS_AQ.DEQUEUE(
				queue_name => :2,
				dequeue_options => v_dequeue_options,
				message_properties => v_message_properties,
				payload => temp_exception_queue_pkg.g_payload,
				msgid => temp_exception_queue_pkg.g_msgid
			);
			
			-- Устанавливаем флаг успеха
			temp_exception_queue_pkg.g_success := 1;
			temp_exception_queue_pkg.g_error_code := 0;
			temp_exception_queue_pkg.g_error_msg := NULL;
		EXCEPTION
			WHEN OTHERS THEN
				temp_exception_queue_pkg.g_error_code := SQLCODE;
				temp_exception_queue_pkg.g_error_msg := SUBSTR(SQLERRM, 1, 4000);
				IF SQLCODE = -25228 THEN
					-- Очередь пуста - это нормально
					temp_exception_queue_pkg.g_success := 0;
				ELSE
					-- Другая ошибка - поднимаем исключение
					temp_exception_queue_pkg.g_success := 0;
					RAISE;
				END IF;
		END;
	`

	// Отмечаем начало операции с БД для предотвращения переподключения во время транзакции
	eqr.dbConn.BeginOperation()
	defer eqr.dbConn.EndOperation()

	// Создаем контекст с таймаутом для транзакции, объединяя с переданным контекстом
	// Это позволяет отменить транзакцию при graceful shutdown
	// Таймаут = waitTimeout + небольшой запас для выполнения операций
	txTimeout := time.Duration(waitTimeout)*time.Second + 5*time.Second
	if txTimeout > execTimeout {
		txTimeout = execTimeout
	}
	// Объединяем переданный контекст с таймаутом для возможности отмены
	txCtx, txCancel := context.WithTimeout(ctx, txTimeout)
	defer txCancel()

	// Выполняем операции в транзакции для обеспечения атомарности
	tx, err := eqr.dbConn.db.BeginTx(txCtx, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback() // Откатываем, если что-то пойдет не так

	// Выполняем PL/SQL блок для dequeue (без consumer_name)
	_, err = tx.ExecContext(txCtx, plsql,
		waitTimeout, // :1
		queueName,   // :2
	)

	if err != nil {
		// Проверяем, была ли ошибка из-за отмены контекста (graceful shutdown)
		isContextCanceled := ctx.Err() == context.Canceled || txCtx.Err() == context.Canceled

		// Откатываем транзакцию при ошибке (если она еще не откачена)
		if !isContextCanceled {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
				// Игнорируем ошибку "transaction has already been committed or rolled back"
				if !strings.Contains(rollbackErr.Error(), "already been committed or rolled back") {
					logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
				}
			}
		}

		// Проверяем, не пуста ли очередь
		errStr := err.Error()
		if strings.Contains(errStr, "25228") || strings.Contains(errStr, "-25228") {
			if logger.Log != nil {
				logger.Log.Debug("Exception queue пуста (код ошибки 25228)")
			}
			return nil, nil
		}

		// Если ошибка из-за отмены контекста - это нормально при graceful shutdown
		if isContextCanceled {
			if logger.Log != nil {
				logger.Log.Info("Операция dequeue из exception queue отменена из-за graceful shutdown",
					zap.String("queue", queueName))
			}
			return nil, fmt.Errorf("операция отменена: %w", ctx.Err())
		}

		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения PL/SQL для dequeue из exception queue",
				zap.Error(err),
				zap.String("queue", queueName),
				zap.Int("timeout", waitTimeout))
		}
		return nil, fmt.Errorf("ошибка выполнения PL/SQL: %w", err)
	}

	// Проверяем успешность dequeue через функции пакета
	checkSuccessSQL := `SELECT temp_exception_queue_pkg.get_success(), temp_exception_queue_pkg.get_error_code(), temp_exception_queue_pkg.get_error_msg() FROM DUAL`
	var successFlag, errorCode sql.NullInt64
	var errorMsg sql.NullString
	err = tx.QueryRowContext(txCtx, checkSuccessSQL).Scan(&successFlag, &errorCode, &errorMsg)
	if err != nil {
		// Проверяем, была ли ошибка из-за отмены контекста
		isContextCanceled := ctx.Err() == context.Canceled || txCtx.Err() == context.Canceled

		if !isContextCanceled {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
				// Игнорируем ошибку "transaction has already been committed or rolled back"
				if !strings.Contains(rollbackErr.Error(), "already been committed or rolled back") {
					logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
				}
			}
		}

		if isContextCanceled {
			if logger.Log != nil {
				logger.Log.Info("Проверка результата dequeue из exception queue отменена из-за graceful shutdown")
			}
			return nil, fmt.Errorf("операция отменена: %w", ctx.Err())
		}

		return nil, fmt.Errorf("ошибка проверки результата dequeue: %w", err)
	}

	// Если dequeue не удался (очередь пуста)
	if !successFlag.Valid || successFlag.Int64 == 0 {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if errorCode.Valid && errorCode.Int64 == -25228 {
			return nil, nil // Очередь пуста
		}
		errText := "неизвестная ошибка"
		if errorMsg.Valid && errorMsg.String != "" {
			errText = errorMsg.String
		}
		return nil, fmt.Errorf("ошибка Oracle (код %d): %s", errorCode.Int64, errText)
	}

	// Используем XMLSerialize для получения CLOB из XMLType
	query := `SELECT RAWTOHEX(temp_exception_queue_pkg.get_msgid()) as msgid, 
	             XMLSerialize(DOCUMENT temp_exception_queue_pkg.get_payload() AS CLOB) as payload 
	          FROM DUAL`

	rows, err := tx.QueryContext(txCtx, query)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения SELECT с XMLSerialize", zap.Error(err))
		}
		return nil, fmt.Errorf("ошибка выполнения SELECT с XMLSerialize: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		return nil, nil
	}

	var msgid, payload sql.NullString
	if err := rows.Scan(&msgid, &payload); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		return nil, fmt.Errorf("ошибка чтения данных: %w", err)
	}

	if !payload.Valid || payload.String == "" {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		return nil, nil
	}

	xmlString := payload.String

	msgidStr := ""
	if msgid.Valid {
		msgidStr = msgid.String
	}

	// Коммитим транзакцию только после успешного чтения данных
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	msg := &QueueMessage{
		MessageID:   msgidStr,
		XMLPayload:  xmlString,
		RawPayload:  []byte(xmlString),
		DequeueTime: time.Now(),
	}

	if logger.Log != nil {
		logger.Log.Debug("Получено сообщение из exception queue",
			zap.String("messageID", msg.MessageID),
			zap.Int("size", len(msg.RawPayload)))
	}
	return msg, nil
}

// ParseXMLMessage парсит XML сообщение из exception queue
// Использует ту же логику парсинга, что и обычная очередь
func (eqr *ExceptionQueueReader) ParseXMLMessage(msg *QueueMessage) (map[string]interface{}, error) {
	// Используем ту же функцию парсинга из queue_reader.go
	// Создаем временный QueueReader для доступа к методу ParseXMLMessage
	tempReader := &QueueReader{
		dbConn: eqr.dbConn,
	}
	return tempReader.ParseXMLMessage(msg)
}

// GetQueueName возвращает имя exception queue
func (eqr *ExceptionQueueReader) GetQueueName() string {
	return eqr.queueName
}

// SetWaitTimeout устанавливает таймаут ожидания сообщений
func (eqr *ExceptionQueueReader) SetWaitTimeout(seconds int) {
	eqr.mu.Lock()
	defer eqr.mu.Unlock()
	eqr.waitTimeout = seconds
}
