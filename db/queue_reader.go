package db

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// QueueMessage представляет сообщение из очереди Oracle AQ
type QueueMessage struct {
	MessageID   string
	XMLPayload  string
	RawPayload  []byte
	DequeueTime time.Time
}

// QueueReader инкапсулирует работу с очередью Oracle AQ
type QueueReader struct {
	dbConn       *DBConnection
	queueName    string
	consumerName string
	waitTimeout  int // в секундах
	mu           sync.Mutex
}

// NewQueueReader создает новый экземпляр QueueReader
func NewQueueReader(dbConn *DBConnection) (*QueueReader, error) {
	cfg := dbConn.cfg
	if cfg == nil {
		return nil, errors.New("конфигурация не загружена")
	}

	queueSec := cfg.Section("queue")
	queueName := queueSec.Key("queue_name").String()
	if queueName == "" {
		queueName = "ASKAQ.AQ_ASK" // значение по умолчанию
	}

	consumerName := queueSec.Key("consumer_name").String()
	if consumerName == "" {
		consumerName = "" // Пустой consumer - читаем без указания consumer
	}

	return &QueueReader{
		dbConn:       dbConn,
		queueName:    queueName,
		consumerName: consumerName,
		waitTimeout:  2, // 2 секунды по умолчанию
	}, nil
}

// DequeueMany извлекает несколько сообщений из очереди (аналогично queue.deqmany() в Python)
// Возвращает слайс сообщений, может быть пустым если очередь пуста
// По аналогии с Python: queue.deqmany(settings.query_number)
// Принимает контекст для возможности отмены операций при graceful shutdown
func (qr *QueueReader) DequeueMany(ctx context.Context, count int) ([]*QueueMessage, error) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	if qr.dbConn.db == nil {
		return nil, errors.New("соединение с БД не открыто")
	}

	if count <= 0 {
		count = 1
	}

	// Один лог о попытке получить N сообщений
	consumerName := qr.consumerName
	if consumerName == "" {
		consumerName = "NULL"
	}
	if logger.Log != nil {
		logger.Log.Debug("Попытка извлечения сообщений из очереди",
			zap.Int("count", count),
			zap.String("queue", qr.queueName),
			zap.String("consumer", consumerName),
			zap.Int("timeout", qr.waitTimeout))
	}

	// Создаем контекст с таймаутом для операций (объединяем с переданным контекстом)
	// Это позволяет отменить операции при graceful shutdown
	opCtx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	// Создаем пакет один раз перед извлечением всех сообщений
	if err := qr.ensurePackageExists(opCtx); err != nil {
		return nil, fmt.Errorf("ошибка создания пакета: %w", err)
	}

	var messages []*QueueMessage

	// Извлекаем сообщения по одному (аналогично Python, где deqmany тоже работает последовательно)
	// Для первого сообщения используем полный waitTimeout, для последующих - минимальный (50 мс),
	// чтобы быстро понять, что очередь пуста, и не ждать 10 секунд
	for i := 0; i < count; i++ {
		// Проверяем контекст перед каждой итерацией для возможности прерывания
		select {
		case <-ctx.Done():
			if logger.Log != nil {
				logger.Log.Info("Операция чтения из очереди прервана",
					zap.Int("received", len(messages)))
			}
			return messages, ctx.Err()
		default:
		}

		// Для первого сообщения используем полный timeout, для остальных - минимальный
		var timeout float64
		if i == 0 {
			timeout = float64(qr.waitTimeout)
		} else {
			// Для последующих сообщений используем минимальный timeout (50 миллисекунд = 0.05 секунды)
			// чтобы быстро определить, что очередь пуста
			timeout = 0.05
		}

		msg, err := qr.dequeueOneMessageWithTimeout(ctx, timeout)
		if err != nil {
			// Если ошибка связана с отменой контекста, возвращаем частичный результат
			if ctx.Err() != nil {
				if logger.Log != nil {
					logger.Log.Info("Операция чтения из очереди прервана из-за отмены контекста",
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

// ensurePackageExists создает пакет Oracle для работы с очередью, если он еще не существует
func (qr *QueueReader) ensurePackageExists(ctx context.Context) error {
	// Создаем пакет с переменными и функциями для доступа к ним
	// В Oracle нельзя напрямую обращаться к переменным пакета в SELECT, нужны функции-геттеры
	createPackageSQL := `
		CREATE OR REPLACE PACKAGE temp_queue_pkg AS
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
		END temp_queue_pkg;
	`
	_, err := qr.dbConn.db.ExecContext(ctx, createPackageSQL)
	if err != nil {
		return fmt.Errorf("не удалось создать пакет: %w", err)
	}

	// Создаем тело пакета с реализацией функций
	createPackageBodySQL := `
		CREATE OR REPLACE PACKAGE BODY temp_queue_pkg AS
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
		END temp_queue_pkg;
	`
	_, err = qr.dbConn.db.ExecContext(ctx, createPackageBodySQL)
	if err != nil {
		return fmt.Errorf("не удалось создать тело пакета: %w", err)
	}

	return nil
}

// dequeueOneMessageWithTimeout извлекает одно сообщение из очереди с указанным timeout (в секундах, может быть дробным)
// По аналогии с Python: queue.deqmany() -> получаем массив сообщений
// Использует DBMS_AQ.DEQUEUE с XMLType payload, аналогично Python connection.queue()
// Использует подход с функцией, возвращающей данные через SELECT с XMLSerialize (аналогично Python)
// Принимает контекст для возможности отмены операций при graceful shutdown
func (qr *QueueReader) dequeueOneMessageWithTimeout(ctx context.Context, timeout float64) (*QueueMessage, error) {
	// Используем подход аналогичный Python:
	// cursor.execute("SELECT XMLSerialize(DOCUMENT :xml AS CLOB) FROM DUAL", xml=message.payload)
	// Создаем функцию, которая делает dequeue и возвращает данные через SELECT с XMLSerialize

	// PL/SQL блок для выполнения dequeue и сохранения результата в пакетные переменные
	plsql := `
		DECLARE
			v_dequeue_options DBMS_AQ.dequeue_options_t;
			v_message_properties DBMS_AQ.message_properties_t;
			v_consumer_name VARCHAR2(128);
		BEGIN
			-- Инициализируем переменные пакета
			temp_queue_pkg.g_success := 0;
			temp_queue_pkg.g_error_code := 0;
			temp_queue_pkg.g_error_msg := NULL;
			
			-- Настраиваем опции dequeue
			v_dequeue_options.dequeue_mode := DBMS_AQ.REMOVE;
			v_dequeue_options.wait := :1;
			v_dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
			
			-- Устанавливаем consumer_name только если он не пустой
			IF :2 IS NOT NULL THEN
				v_consumer_name := :2;
				IF LENGTH(TRIM(v_consumer_name)) > 0 THEN
					v_dequeue_options.consumer_name := TRIM(v_consumer_name);
				END IF;
			END IF;
			
			-- Выполняем dequeue и сохраняем результат в пакетные переменные
			DBMS_AQ.DEQUEUE(
				queue_name => :3,
				dequeue_options => v_dequeue_options,
				message_properties => v_message_properties,
				payload => temp_queue_pkg.g_payload,
				msgid => temp_queue_pkg.g_msgid
			);
			
			-- Устанавливаем флаг успеха
			temp_queue_pkg.g_success := 1;
			temp_queue_pkg.g_error_code := 0;
			temp_queue_pkg.g_error_msg := NULL;
		EXCEPTION
			WHEN OTHERS THEN
				temp_queue_pkg.g_error_code := SQLCODE;
				temp_queue_pkg.g_error_msg := SUBSTR(SQLERRM, 1, 4000);
				IF SQLCODE = -25228 THEN
					-- Очередь пуста - это нормально
					temp_queue_pkg.g_success := 0;
				ELSE
					-- Другая ошибка - поднимаем исключение
					temp_queue_pkg.g_success := 0;
					RAISE;
				END IF;
		END;
	`

	// Подготавливаем параметр consumer_name
	var consumerParam interface{}
	if qr.consumerName == "" {
		consumerParam = nil // NULL для Oracle
	} else {
		consumerParam = strings.TrimSpace(qr.consumerName)
	}

	// Отмечаем начало операции с БД для предотвращения переподключения во время транзакции
	qr.dbConn.BeginOperation()
	defer qr.dbConn.EndOperation()

	// Создаем контекст с таймаутом для транзакции, объединяя с переданным контекстом
	// Это позволяет отменить транзакцию при graceful shutdown
	// Таймаут = timeout запроса + небольшой запас для выполнения операций
	txTimeout := time.Duration(timeout)*time.Second + 5*time.Second
	if txTimeout > execTimeout {
		txTimeout = execTimeout
	}
	// Объединяем переданный контекст с таймаутом для возможности отмены
	txCtx, txCancel := context.WithTimeout(ctx, txTimeout)
	defer txCancel()

	// Выполняем операции в транзакции для обеспечения атомарности
	// Это важно для правильной работы с REMOVE режимом dequeue
	tx, err := qr.dbConn.db.BeginTx(txCtx, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback() // Откатываем, если что-то пойдет не так

	// Выполняем PL/SQL блок для dequeue
	_, err = tx.ExecContext(txCtx, plsql,
		timeout,       // :1 - используем переданный timeout
		consumerParam, // :2
		qr.queueName,  // :3
	)

	if err != nil {
		// Откатываем транзакцию при ошибке
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		// Проверяем, не пуста ли очередь
		errStr := err.Error()
		if strings.Contains(errStr, "25228") || strings.Contains(errStr, "-25228") {
			if logger.Log != nil {
				logger.Log.Debug("Очередь пуста (код ошибки 25228)")
			}
			return nil, nil
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения PL/SQL для dequeue",
				zap.Error(err),
				zap.String("consumer", qr.consumerName),
				zap.String("queue", qr.queueName),
				zap.Float64("timeout", timeout))
		}
		return nil, fmt.Errorf("ошибка выполнения PL/SQL: %w", err)
	}

	// Проверяем успешность dequeue через функции пакета
	checkSuccessSQL := `SELECT temp_queue_pkg.get_success(), temp_queue_pkg.get_error_code(), temp_queue_pkg.get_error_msg() FROM DUAL`
	var successFlag, errorCode sql.NullInt64
	var errorMsg sql.NullString
	err = tx.QueryRowContext(txCtx, checkSuccessSQL).Scan(&successFlag, &errorCode, &errorMsg)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
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

	// Используем XMLSerialize для получения CLOB из XMLType (аналогично Python)
	// SELECT XMLSerialize(DOCUMENT :xml AS CLOB) FROM DUAL
	// Используем функции пакета для доступа к переменным
	query := `SELECT RAWTOHEX(temp_queue_pkg.get_msgid()) as msgid, 
	             XMLSerialize(DOCUMENT temp_queue_pkg.get_payload() AS CLOB) as payload 
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

	// В godror при использовании XMLSerialize и Scan в sql.NullString,
	// CLOB автоматически читается и возвращается как строка
	// Аналогично Python: xml_string = cursor.fetchone()[0]
	// и если hasattr(xml_string, "read"): xml_string = xml_string.read()
	xmlString := payload.String

	msgidStr := ""
	if msgid.Valid {
		msgidStr = msgid.String
	}

	// Коммитим транзакцию только после успешного чтения данных
	// Это подтверждает удаление сообщения из очереди (dequeue_mode = REMOVE)
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
		logger.Log.Debug("Получено сообщение из очереди",
			zap.String("messageID", msg.MessageID),
			zap.Int("size", len(msg.RawPayload)))
	}
	return msg, nil
}

// getQueueTableName извлекает имя таблицы очереди из имени очереди
func (qr *QueueReader) getQueueTableName() string {
	// Формат: SCHEMA.QUEUE_NAME -> обычно SCHEMA.QUEUE_NAME_TABLE
	parts := strings.Split(qr.queueName, ".")
	if len(parts) == 2 {
		schema := parts[0]
		queueName := parts[1]
		// Обычно таблица имеет суффикс _TABLE
		return fmt.Sprintf("%s.%s_TABLE", schema, queueName)
	}
	return ""
}

// ParseXMLMessage парсит XML сообщение из очереди
// Возвращает map с данными из XML
// Структура XML: /root/head/date_active_from и /root/body (содержит внутренний XML)
// Внутренний XML содержит элементы: sms_task_id, phone_number, message, sender_name, sending_schedule, smpp_id
func (qr *QueueReader) ParseXMLMessage(msg *QueueMessage) (map[string]interface{}, error) {
	if msg == nil || msg.XMLPayload == "" {
		return nil, errors.New("сообщение пусто или не содержит XML")
	}

	// Парсим корневой элемент root
	type Root struct {
		XMLName xml.Name `xml:"root"`
		Head    struct {
			DateActiveFrom string `xml:"date_active_from"`
		} `xml:"head"`
		Body struct {
			InnerXML string `xml:",innerxml"`
		} `xml:"body"`
	}

	var root Root
	xmlBytes := []byte(msg.XMLPayload)
	if err := xml.Unmarshal(xmlBytes, &root); err != nil {
		return nil, fmt.Errorf("ошибка парсинга корневого XML: %w, XML: %s", err, truncateString(msg.XMLPayload, 500))
	}

	// Парсим внутренний XML из body (аналогично C# коду: xmlDocInner.LoadXml(xmlDoc.DocumentElement.SelectSingleNode("/root/body").InnerText))
	// Внутренний XML содержит элементы: sms_task_id, phone_number, message, sender_name, sending_schedule, smpp_id
	type SMSData struct {
		SmsTaskID       string `xml:"sms_task_id"`
		PhoneNumber     string `xml:"phone_number"`
		Message         string `xml:"message"`
		SenderName      string `xml:"sender_name"`
		SendingSchedule string `xml:"sending_schedule"`
		SmppID          string `xml:"smpp_id"`
	}

	var smsData SMSData
	bodyXML := strings.TrimSpace(root.Body.InnerXML)

	// Если body пуст, возможно структура XML другая - пробуем парсить напрямую
	if bodyXML == "" {
		// Пробуем парсить весь XML как SMSData напрямую
		if err := xml.Unmarshal(xmlBytes, &smsData); err != nil {
			return nil, fmt.Errorf("ошибка парсинга XML (body пуст, пробуем прямой парсинг): %w, XML: %s", err, truncateString(msg.XMLPayload, 500))
		}
	} else {
		// Извлекаем содержимое из CDATA, если оно там есть
		// CDATA формат: <![CDATA[содержимое]]>
		bodyXML = extractCDATAContent(bodyXML)

		// Парсим внутренний XML из body
		if err := xml.Unmarshal([]byte(bodyXML), &smsData); err != nil {
			return nil, fmt.Errorf("ошибка парсинга внутреннего XML из body: %w, body content: %s", err, truncateString(bodyXML, 500))
		}
	}

	result := map[string]interface{}{
		"message_id":       msg.MessageID,
		"dequeue_time":     msg.DequeueTime,
		"date_active_from": root.Head.DateActiveFrom,
		"sms_task_id":      smsData.SmsTaskID,
		"phone_number":     smsData.PhoneNumber,
		"message":          smsData.Message,
		"sender_name":      smsData.SenderName,
		"sending_schedule": smsData.SendingSchedule,
		"smpp_id":          smsData.SmppID,
	}

	return result, nil
}

// extractCDATAContent извлекает содержимое из CDATA секции, если оно там есть
// CDATA формат: <![CDATA[содержимое]]>
// Обрабатывает различные варианты: CDATA в начале/конце, с пробелами, уже развернутый XML
func extractCDATAContent(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}

	cdataStart := "<![CDATA["
	cdataEnd := "]]>"

	// Ищем начало CDATA в любом месте строки
	startIdx := strings.Index(s, cdataStart)
	if startIdx != -1 {
		// Нашли начало CDATA - ищем конец после начала
		endIdx := strings.Index(s[startIdx+len(cdataStart):], cdataEnd)
		if endIdx != -1 {
			// Извлекаем содержимое между <![CDATA[ и ]]>
			contentStart := startIdx + len(cdataStart)
			contentEnd := startIdx + len(cdataStart) + endIdx
			content := s[contentStart:contentEnd]
			return strings.TrimSpace(content)
		}
	}

	// Если CDATA не найден, возможно содержимое уже развернуто
	// Проверяем, является ли строка валидным XML (начинается с <)
	if strings.HasPrefix(strings.TrimSpace(s), "<") {
		// Содержимое уже развернуто, возвращаем как есть
		return s
	}

	// Если ничего не подошло, возвращаем исходную строку
	return s
}

// truncateString обрезает строку до указанной длины (вспомогательная функция)
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// getStringValue безопасно преобразует значение в строку, обрабатывая nil
func getStringValue(v interface{}) string {
	if v == nil {
		return "<NULL>"
	}
	switch val := v.(type) {
	case sql.NullString:
		if !val.Valid {
			return "<NULL>"
		}
		return val.String
	default:
		return fmt.Sprintf("%v", v)
	}
}

// GetQueueName возвращает имя очереди
func (qr *QueueReader) GetQueueName() string {
	return qr.queueName
}

// GetConsumerName возвращает имя consumer
func (qr *QueueReader) GetConsumerName() string {
	return qr.consumerName
}

// SetWaitTimeout устанавливает таймаут ожидания сообщений
func (qr *QueueReader) SetWaitTimeout(seconds int) {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	qr.waitTimeout = seconds
}
