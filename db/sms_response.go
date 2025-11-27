package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// SaveSmsResponseParams представляет параметры для вызова процедуры save_sms_response
type SaveSmsResponseParams struct {
	TaskID       int64     // P_SMS_TASK_ID (может быть -1 для NULL)
	MessageID    string    // P_MESSAGE_ID
	StatusID     int       // P_STATUS_ID (2 - успех, 3 - ошибка)
	ResponseDate time.Time // P_DATE_RESPONSE
	ErrorText    string    // P_ERROR_TEXT (может быть пустой для NULL)
}

// SaveSmsResponse вызывает процедуру pcsystem.pkg_sms.save_sms_response() для сохранения результатов отправки SMS
// Использует блокировку для потокобезопасности
// Принимает контекст для возможности отмены операций при graceful shutdown
// Возвращает true при успешном выполнении, false при ошибке
func (d *DBConnection) SaveSmsResponse(ctx context.Context, params SaveSmsResponseParams) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db == nil {
		return false, fmt.Errorf("соединение с БД не открыто")
	}

	// Проверяем соединение
	if !d.CheckConnection() {
		return false, fmt.Errorf("соединение с БД недоступно")
	}

	// Отмечаем начало операции с БД для предотвращения переподключения во время транзакции
	d.BeginOperation()
	defer d.EndOperation()

	// Создаем контекст с таймаутом для транзакции
	// Если основной контекст отменен (graceful shutdown), используем background context
	// чтобы дать время на завершение критических операций (сохранение в БД)
	var queryCtx context.Context
	var queryCancel context.CancelFunc
	if ctx.Err() == context.Canceled {
		// Контекст уже отменен - используем background context с таймаутом
		// чтобы дать время на завершение операции
		queryCtx, queryCancel = context.WithTimeout(context.Background(), execTimeout)
	} else {
		// Контекст активен - используем его с таймаутом
		queryCtx, queryCancel = context.WithTimeout(ctx, execTimeout)
	}
	defer queryCancel()

	// Подготовка параметров
	var taskID interface{}
	if params.TaskID == -1 {
		taskID = nil // NULL в Oracle
	} else {
		taskID = params.TaskID
	}

	var errorText interface{}
	if params.ErrorText == "" {
		errorText = nil // NULL в Oracle
	} else {
		errorText = params.ErrorText
	}

	// Создаем временный пакет для хранения OUT-параметров (аналогично queue_reader.go)
	if err := d.ensureSmsResponsePackageExists(queryCtx); err != nil {
		return false, fmt.Errorf("ошибка создания пакета: %w", err)
	}

	// Выполняем операции в транзакции для обеспечения атомарности
	tx, err := d.db.BeginTx(queryCtx, nil)
	if err != nil {
		return false, fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback() // Откатываем, если что-то пойдет не так

	// Используем подход аналогичный queue_reader.go - сохраняем OUT-параметры в пакетные переменные
	// через PL/SQL блок, затем читаем их через SELECT
	plsql := `
		DECLARE
			v_err_code NUMBER;
			v_err_desc VARCHAR2(4000);
		BEGIN
			-- Инициализируем переменные пакета
			temp_sms_response_pkg.g_err_code := 0;
			temp_sms_response_pkg.g_err_desc := NULL;
			
			-- Вызываем процедуру и сохраняем OUT-параметры в локальные переменные
			pcsystem.pkg_sms.save_sms_response(
				P_SMS_TASK_ID => :1,
				P_MESSAGE_ID => :2,
				P_STATUS_ID => :3,
				P_DATE_RESPONSE => :4,
				P_ERROR_TEXT => :5,
				P_ERR_CODE => v_err_code,
				P_ERR_DESC => v_err_desc
			);
			
			-- Сохраняем результат в пакетные переменные
			temp_sms_response_pkg.g_err_code := v_err_code;
			temp_sms_response_pkg.g_err_desc := v_err_desc;
		END;`

	_, err = tx.ExecContext(queryCtx, plsql,
		taskID,
		params.MessageID,
		params.StatusID,
		params.ResponseDate,
		errorText,
	)

	if err != nil {
		// Проверяем, была ли операция отменена из-за graceful shutdown
		// Но только если queryCtx тоже отменен (таймаут истек)
		// Это позволяет дать время на завершение критических операций при graceful shutdown
		if queryCtx.Err() != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
				logger.Log.Warn("Ошибка отката транзакции при отмене контекста", zap.Error(rollbackErr))
			}
			if logger.Log != nil {
				if ctx.Err() == context.Canceled {
					logger.Log.Warn("Сохранение результата SMS отменено из-за graceful shutdown",
						zap.Int64("taskID", params.TaskID),
						zap.String("messageID", params.MessageID))
				} else {
					logger.Log.Warn("Сохранение результата SMS отменено из-за таймаута",
						zap.Int64("taskID", params.TaskID),
						zap.String("messageID", params.MessageID))
				}
			}
			return false, fmt.Errorf("операция отменена: %w", queryCtx.Err())
		}

		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка вызова pcsystem.pkg_sms.save_sms_response",
				zap.Int64("taskID", params.TaskID),
				zap.String("messageID", params.MessageID),
				zap.Int("statusID", params.StatusID),
				zap.Error(err))
		}
		return false, fmt.Errorf("ошибка вызова save_sms_response: %w", err)
	}

	// Читаем результат через функции пакета (аналогично queue_reader.go)
	checkResultSQL := `SELECT temp_sms_response_pkg.get_err_code(), temp_sms_response_pkg.get_err_desc() FROM DUAL`
	var errCode sql.NullInt64
	var errDesc sql.NullString
	err = tx.QueryRowContext(queryCtx, checkResultSQL).Scan(&errCode, &errDesc)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка чтения результата процедуры",
				zap.Int64("taskID", params.TaskID),
				zap.String("messageID", params.MessageID),
				zap.Error(err))
		}
		return false, fmt.Errorf("ошибка чтения результата процедуры: %w", err)
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		// Проверяем, была ли операция отменена из-за graceful shutdown или таймаута
		if queryCtx.Err() != nil {
			if logger.Log != nil {
				if ctx.Err() == context.Canceled {
					logger.Log.Warn("Коммит транзакции отменен из-за graceful shutdown",
						zap.Int64("taskID", params.TaskID),
						zap.String("messageID", params.MessageID))
				} else {
					logger.Log.Warn("Коммит транзакции отменен из-за таймаута",
						zap.Int64("taskID", params.TaskID),
						zap.String("messageID", params.MessageID))
				}
			}
			return false, fmt.Errorf("операция отменена: %w", queryCtx.Err())
		}
		return false, fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	// Проверка результата процедуры
	if errCode.Valid && errCode.Int64 != 0 {
		errMsg := ""
		if errDesc.Valid {
			errMsg = errDesc.String
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения pcsystem.pkg_sms.save_sms_response",
				zap.Int64("errCode", errCode.Int64),
				zap.String("errDesc", errMsg),
				zap.Int64("taskID", params.TaskID),
				zap.String("messageID", params.MessageID),
				zap.Int("statusID", params.StatusID))
		}
		return false, fmt.Errorf("ошибка БД: %d - %s", errCode.Int64, errMsg)
	}

	// Успешное выполнение
	if logger.Log != nil {
		logger.Log.Info("Вызов pcsystem.pkg_sms.save_sms_response() успешно",
			zap.Int64("taskID", params.TaskID),
			zap.String("messageID", params.MessageID),
			zap.Int("statusID", params.StatusID),
			zap.Time("responseDate", params.ResponseDate),
			zap.String("errorText", params.ErrorText))
	}

	return true, nil
}

// SaveSmsResponseBatch сохраняет несколько результатов SMS за одну транзакцию
// Возвращает количество успешно сохраненных записей и ошибку (если есть)
func (d *DBConnection) SaveSmsResponseBatch(ctx context.Context, params []SaveSmsResponseParams) (int, error) {
	if len(params) == 0 {
		return 0, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db == nil {
		return 0, fmt.Errorf("соединение с БД не открыто")
	}

	// Проверяем соединение
	if !d.CheckConnection() {
		return 0, fmt.Errorf("соединение с БД недоступно")
	}

	// Отмечаем начало операции с БД
	d.BeginOperation()
	defer d.EndOperation()

	// Создаем контекст с увеличенным таймаутом для batch-операции
	var queryCtx context.Context
	var queryCancel context.CancelFunc
	batchTimeout := execTimeout * 2 // Удвоенный таймаут для batch
	if ctx.Err() == context.Canceled {
		queryCtx, queryCancel = context.WithTimeout(context.Background(), batchTimeout)
	} else {
		queryCtx, queryCancel = context.WithTimeout(ctx, batchTimeout)
	}
	defer queryCancel()

	// Создаем пакет один раз для всех записей
	if err := d.ensureSmsResponsePackageExists(queryCtx); err != nil {
		return 0, fmt.Errorf("ошибка создания пакета: %w", err)
	}

	// Начинаем транзакцию
	tx, err := d.db.BeginTx(queryCtx, nil)
	if err != nil {
		return 0, fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback()

	// PL/SQL блок для вызова процедуры
	plsql := `
		DECLARE
			v_err_code NUMBER;
			v_err_desc VARCHAR2(4000);
		BEGIN
			temp_sms_response_pkg.g_err_code := 0;
			temp_sms_response_pkg.g_err_desc := NULL;
			
			pcsystem.pkg_sms.save_sms_response(
				P_SMS_TASK_ID => :1,
				P_MESSAGE_ID => :2,
				P_STATUS_ID => :3,
				P_DATE_RESPONSE => :4,
				P_ERROR_TEXT => :5,
				P_ERR_CODE => v_err_code,
				P_ERR_DESC => v_err_desc
			);
			
			temp_sms_response_pkg.g_err_code := v_err_code;
			temp_sms_response_pkg.g_err_desc := v_err_desc;
		END;`

	successCount := 0
	var lastErr error

	for _, p := range params {
		// Подготовка параметров
		var taskID interface{}
		if p.TaskID == -1 {
			taskID = nil
		} else {
			taskID = p.TaskID
		}

		var errorText interface{}
		if p.ErrorText == "" {
			errorText = nil
		} else {
			errorText = p.ErrorText
		}

		// Выполняем процедуру
		_, err := tx.ExecContext(queryCtx, plsql,
			taskID,
			p.MessageID,
			p.StatusID,
			p.ResponseDate,
			errorText,
		)

		if err != nil {
			if logger.Log != nil {
				logger.Log.Warn("Ошибка сохранения записи в batch",
					zap.String("messageID", p.MessageID),
					zap.Error(err))
			}
			lastErr = err
			continue
		}

		successCount++
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return successCount, fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	if logger.Log != nil && successCount > 0 {
		logger.Log.Info("Batch сохранение результатов SMS завершено",
			zap.Int("total", len(params)),
			zap.Int("success", successCount))
	}

	return successCount, lastErr
}

// ensureSmsResponsePackageExists создает временный пакет Oracle для работы с OUT-параметрами процедуры save_sms_response
// Аналогично queue_reader.go - Oracle требует использования пакетных переменных для передачи данных
func (d *DBConnection) ensureSmsResponsePackageExists(ctx context.Context) error {
	// Создаем пакет с переменными и функциями для доступа к ним
	createPackageSQL := `
		CREATE OR REPLACE PACKAGE temp_sms_response_pkg AS
			g_err_code NUMBER := 0;
			g_err_desc VARCHAR2(4000);
			
			FUNCTION get_err_code RETURN NUMBER;
			FUNCTION get_err_desc RETURN VARCHAR2;
		END temp_sms_response_pkg;
	`
	_, err := d.db.ExecContext(ctx, createPackageSQL)
	if err != nil {
		return fmt.Errorf("не удалось создать пакет temp_sms_response_pkg: %w", err)
	}

	// Создаем тело пакета с реализацией функций-геттеров
	createPackageBodySQL := `
		CREATE OR REPLACE PACKAGE BODY temp_sms_response_pkg AS
			FUNCTION get_err_code RETURN NUMBER IS
			BEGIN
				RETURN g_err_code;
			END;
			
			FUNCTION get_err_desc RETURN VARCHAR2 IS
			BEGIN
				RETURN g_err_desc;
			END;
		END temp_sms_response_pkg;
	`
	_, err = d.db.ExecContext(ctx, createPackageBodySQL)
	if err != nil {
		return fmt.Errorf("не удалось создать тело пакета temp_sms_response_pkg: %w", err)
	}

	return nil
}
