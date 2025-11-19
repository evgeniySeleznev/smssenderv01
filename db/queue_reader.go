package db

import (
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
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
func (qr *QueueReader) DequeueMany(count int) ([]*QueueMessage, error) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	if qr.dbConn.db == nil {
		return nil, errors.New("соединение с БД не открыто")
	}

	if count <= 0 {
		count = 1
	}

	var messages []*QueueMessage

	// Извлекаем сообщения по одному (аналогично Python, где deqmany тоже работает последовательно)
	for i := 0; i < count; i++ {
		msg, err := qr.dequeueOneMessage()
		if err != nil {
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

// dequeueOneMessage извлекает одно сообщение из очереди
// По аналогии с Python: queue.deqmany() -> получаем массив сообщений
// Использует DBMS_AQ.DEQUEUE с XMLType payload, аналогично Python connection.queue()
// Использует подход с функцией, возвращающей данные через SELECT, чтобы избежать проблем с OUT параметрами CLOB
func (qr *QueueReader) dequeueOneMessage() (*QueueMessage, error) {
	// Используем подход с функцией, которая делает dequeue и возвращает данные через SELECT
	// Это позволяет избежать проблем с OUT параметрами для больших CLOB
	// Создаем временную функцию, которая возвращает данные через SYS_REFCURSOR

	// Используем подход с временной таблицей для хранения результата
	// Это позволяет избежать проблем с OUT параметрами для больших CLOB
	// Аналогично Python, где payload доступен напрямую из объекта сообщения

	// Создаем временную таблицу для хранения результата (если её еще нет)
	createTableSQL := `
		BEGIN
			EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE temp_queue_result (
				msgid VARCHAR2(32),
				payload CLOB
			) ON COMMIT DELETE ROWS';
		EXCEPTION
			WHEN OTHERS THEN
				IF SQLCODE = -955 THEN
					NULL; -- Таблица уже существует
				ELSE
					RAISE;
				END IF;
		END;
	`
	_, _ = qr.dbConn.db.ExecContext(qr.dbConn.ctx, createTableSQL)

	consumerName := qr.consumerName
	if consumerName == "" {
		consumerName = "NULL"
	}
	log.Printf("Попытка извлечения сообщения из очереди %s (consumer: %s, timeout: %d сек)",
		qr.queueName, consumerName, qr.waitTimeout)

	// Отладочный запрос: проверяем, есть ли сообщения в очереди
	// Получаем имя таблицы очереди
	queueTable := qr.getQueueTableName()
	if queueTable != "" {
		// Проверяем все сообщения в очереди
		checkQueryAll := fmt.Sprintf(`
			SELECT COUNT(*) as msg_count, 
			       COUNT(DISTINCT consumer_name) as consumer_count,
			       LISTAGG(DISTINCT consumer_name, ', ') WITHIN GROUP (ORDER BY consumer_name) as consumers
			FROM %s 
			WHERE state = 0
		`, queueTable)

		checkRows, err := qr.dbConn.db.QueryContext(qr.dbConn.ctx, checkQueryAll)
		if err == nil {
			if checkRows.Next() {
				var msgCount, consumerCount sql.NullInt64
				var consumers sql.NullString
				if err := checkRows.Scan(&msgCount, &consumerCount, &consumers); err == nil {
					if msgCount.Valid {
						log.Printf("Отладка: в таблице очереди найдено %d сообщений (state=0)", msgCount.Int64)
						if consumerCount.Valid && consumerCount.Int64 > 0 {
							log.Printf("Отладка: найдено %d различных consumers: %s", consumerCount.Int64, getStringValue(consumers))
						}
					}
				}
			}
			checkRows.Close()
		}

		// Проверяем сообщения для конкретного consumer
		if qr.consumerName != "" {
			checkQuery := fmt.Sprintf(`
				SELECT COUNT(*) as msg_count 
				FROM %s 
				WHERE state = 0 
				AND consumer_name = :consumer_name
			`, queueTable)

			checkRows, err := qr.dbConn.db.QueryContext(qr.dbConn.ctx, checkQuery,
				sql.Named("consumer_name", qr.consumerName),
			)
			if err == nil {
				if checkRows.Next() {
					var msgCount sql.NullInt64
					if err := checkRows.Scan(&msgCount); err == nil && msgCount.Valid {
						log.Printf("Отладка: для consumer '%s' найдено %d сообщений", qr.consumerName, msgCount.Int64)
					}
				}
				checkRows.Close()
			}
		}
	}

	// Используем подход с временной таблицей: делаем dequeue и сохраняем в таблицу, затем читаем
	plsql := `
		DECLARE
			v_dequeue_options DBMS_AQ.dequeue_options_t;
			v_message_properties DBMS_AQ.message_properties_t;
			v_msgid RAW(16);
			v_payload XMLType;
		BEGIN
			-- Очищаем временную таблицу
			DELETE FROM temp_queue_result;
			
			IF :consumer_name IS NOT NULL AND LENGTH(:consumer_name) > 0 THEN
				v_dequeue_options.consumer_name := :consumer_name;
			END IF;
			v_dequeue_options.dequeue_mode := DBMS_AQ.REMOVE;
			v_dequeue_options.wait := :wait_timeout;
			v_dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
			
			DBMS_AQ.DEQUEUE(
				queue_name => :queue_name,
				dequeue_options => v_dequeue_options,
				message_properties => v_message_properties,
				payload => v_payload,
				msgid => v_msgid
			);
			
			-- Сохраняем результат во временную таблицу
			INSERT INTO temp_queue_result (msgid, payload)
			VALUES (RAWTOHEX(v_msgid), v_payload.getClobVal());
			
			COMMIT;
		EXCEPTION
			WHEN OTHERS THEN
				IF SQLCODE = -25228 THEN
					-- Очередь пуста - это нормально
					NULL;
				ELSE
					RAISE;
				END IF;
		END;
	`

	var consumerParam interface{}
	if qr.consumerName == "" {
		consumerParam = sql.NullString{Valid: false}
	} else {
		consumerParam = qr.consumerName
	}

	_, err := qr.dbConn.db.ExecContext(qr.dbConn.ctx, plsql,
		sql.Named("consumer_name", consumerParam),
		sql.Named("wait_timeout", qr.waitTimeout),
		sql.Named("queue_name", qr.queueName),
	)

	if err != nil {
		// Проверяем, не пуста ли очередь
		errStr := err.Error()
		if strings.Contains(errStr, "25228") || strings.Contains(errStr, "-25228") {
			log.Printf("Очередь пуста (код ошибки 25228)")
			return nil, nil
		}
		log.Printf("Ошибка выполнения PL/SQL для dequeue: %v", err)
		log.Printf("Детали ошибки: consumer='%s', queue='%s', timeout=%d", qr.consumerName, qr.queueName, qr.waitTimeout)
		// Пробуем упрощенный подход
		return qr.dequeueOneMessageSimple()
	}

	// Читаем данные из временной таблицы
	query := "SELECT msgid, payload FROM temp_queue_result WHERE ROWNUM = 1"
	rows, err := qr.dbConn.db.QueryContext(qr.dbConn.ctx, query)
	if err != nil {
		log.Printf("Ошибка чтения из временной таблицы: %v", err)
		return nil, fmt.Errorf("ошибка чтения из временной таблицы: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// Временная таблица пуста - значит dequeue не вернул сообщение (очередь пуста)
		log.Printf("Временная таблица пуста - очередь пуста или сообщение не было извлечено")
		return nil, nil
	}

	var msgid, payload sql.NullString
	if err := rows.Scan(&msgid, &payload); err != nil {
		return nil, fmt.Errorf("ошибка чтения данных: %w", err)
	}

	if !payload.Valid || payload.String == "" {
		return nil, nil
	}

	msgidStr := ""
	if msgid.Valid {
		msgidStr = msgid.String
	}

	msg := &QueueMessage{
		MessageID:   msgidStr,
		XMLPayload:  payload.String,
		RawPayload:  []byte(payload.String),
		DequeueTime: time.Now(),
	}

	log.Printf("Получено сообщение из очереди. MessageID: %s, размер: %d байт", msg.MessageID, len(msg.RawPayload))
	return msg, nil
}

// dequeueOneMessageSimple упрощенный подход через PL/SQL с использованием DBMS_SQL для возврата CLOB
func (qr *QueueReader) dequeueOneMessageSimple() (*QueueMessage, error) {
	// Используем упрощенный подход: делаем dequeue и читаем payload через DBMS_LOB
	// Сохраняем результат во временную таблицу или используем пакетную переменную
	plsql := `
		DECLARE
			v_dequeue_options DBMS_AQ.dequeue_options_t;
			v_message_properties DBMS_AQ.message_properties_t;
			v_msgid RAW(16);
			v_payload XMLType;
			v_clob CLOB;
		BEGIN
			IF :consumer_name IS NOT NULL AND LENGTH(:consumer_name) > 0 THEN
				v_dequeue_options.consumer_name := :consumer_name;
			END IF;
			v_dequeue_options.dequeue_mode := DBMS_AQ.REMOVE;
			v_dequeue_options.wait := :wait_timeout;
			v_dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
			
			DBMS_AQ.DEQUEUE(
				queue_name => :queue_name,
				dequeue_options => v_dequeue_options,
				message_properties => v_message_properties,
				payload => v_payload,
				msgid => v_msgid
			);
			
			v_clob := v_payload.getClobVal();
			
			-- Используем DBMS_OUTPUT для передачи данных (ограничение 32767 байт)
			-- Или сохраняем в глобальную переменную пакета
			:msgid_out := RAWTOHEX(v_msgid);
			:payload_out := SUBSTR(v_clob, 1, 4000); -- Ограничиваем размер для OUT параметра
			:success := 1;
		EXCEPTION
			WHEN OTHERS THEN
				IF SQLCODE = -25228 THEN
					:error_code := -25228;
					:success := 0;
				ELSE
					:error_code := SQLCODE;
					:error_msg := SUBSTR(SQLERRM, 1, 4000);
					:success := 0;
				END IF;
		END;
	`

	var msgidOut, payloadOut sql.NullString
	var success sql.NullInt64
	var errorCode sql.NullInt64
	var errorMsg sql.NullString

	var consumerParam interface{}
	if qr.consumerName == "" {
		consumerParam = sql.NullString{Valid: false}
	} else {
		consumerParam = qr.consumerName
	}

	_, err := qr.dbConn.db.ExecContext(qr.dbConn.ctx, plsql,
		sql.Named("consumer_name", consumerParam),
		sql.Named("wait_timeout", qr.waitTimeout),
		sql.Named("queue_name", qr.queueName),
		sql.Out{Dest: &msgidOut},
		sql.Out{Dest: &payloadOut},
		sql.Out{Dest: &success},
		sql.Out{Dest: &errorCode},
		sql.Out{Dest: &errorMsg},
	)

	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения PL/SQL: %w", err)
	}

	if success.Valid && success.Int64 == 0 {
		if errorCode.Valid && errorCode.Int64 == -25228 {
			return nil, nil
		}
		errText := "неизвестная ошибка"
		if errorMsg.Valid {
			errText = errorMsg.String
		}
		return nil, fmt.Errorf("ошибка Oracle (код %d): %s", errorCode.Int64, errText)
	}

	if !payloadOut.Valid || payloadOut.String == "" {
		return nil, nil
	}

	msgid := ""
	if msgidOut.Valid {
		msgid = msgidOut.String
	}

	return &QueueMessage{
		MessageID:   msgid,
		XMLPayload:  payloadOut.String,
		RawPayload:  []byte(payloadOut.String),
		DequeueTime: time.Now(),
	}, nil
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

// DequeueOne извлекает одно сообщение из очереди (для обратной совместимости)
func (qr *QueueReader) DequeueOne() (*QueueMessage, error) {
	messages, err := qr.DequeueMany(1)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, nil
	}
	return messages[0], nil
}

// DequeueAll извлекает все доступные сообщения из очереди
// Возвращает слайс сообщений
func (qr *QueueReader) DequeueAll() ([]*QueueMessage, error) {
	log.Println("Начало выборки всех сообщений из очереди")

	var messages []*QueueMessage
	count := 0

	for {
		msg, err := qr.DequeueOne()
		if err != nil {
			return messages, fmt.Errorf("ошибка при извлечении сообщения: %w", err)
		}

		if msg == nil {
			// Очередь пуста
			break
		}

		messages = append(messages, msg)
		count++

		// Небольшая задержка между сообщениями
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Выборка завершена. Всего прочитано сообщений: %d", count)
	return messages, nil
}

// ParseXMLMessage парсит XML сообщение из очереди
// Возвращает map с данными из XML
// Структура XML: /root/head/date_active_from и /root/body (содержит внутренний XML)
// Внутренний XML содержит элементы: sms_task_id, phone_number, message, sender_name, sending_schedule, smpp_id
func (qr *QueueReader) ParseXMLMessage(msg *QueueMessage) (map[string]interface{}, error) {
	if msg == nil || msg.XMLPayload == "" {
		return nil, errors.New("сообщение пусто или не содержит XML")
	}

	// Логируем исходный XML для отладки
	log.Printf("Парсинг XML сообщения (первые 500 символов): %s", truncateString(msg.XMLPayload, 500))

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
		XMLName         xml.Name `xml:",any"`
		SmsTaskID       string   `xml:"sms_task_id"`
		PhoneNumber     string   `xml:"phone_number"`
		Message         string   `xml:"message"`
		SenderName      string   `xml:"sender_name"`
		SendingSchedule string   `xml:"sending_schedule"`
		SmppID          string   `xml:"smpp_id"`
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
