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

	return &QueueReader{
		dbConn:       dbConn,
		queueName:    queueName,
		consumerName: "SUB_SMS_SENDER",
		waitTimeout:  2, // 2 секунды по умолчанию
	}, nil
}

// DequeueOne извлекает одно сообщение из очереди
// Возвращает nil, nil если очередь пуста (код ошибки 25228)
func (qr *QueueReader) DequeueOne() (*QueueMessage, error) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	if qr.dbConn.db == nil {
		return nil, errors.New("соединение с БД не открыто")
	}

	// PL/SQL блок для извлечения сообщения из очереди
	// Используем DBMS_AQ.DEQUEUE с XMLType payload
	// Очередь использует XMLType (SYS.XMLTYPE), поэтому извлекаем как CLOB
	plsql := `
		DECLARE
			dequeue_options DBMS_AQ.dequeue_options_t;
			message_properties DBMS_AQ.message_properties_t;
			msgid RAW(16);
			payload XMLType;
			xml_str CLOB;
		BEGIN
			-- Настройка опций извлечения
			dequeue_options.consumer_name := :consumer_name;
			dequeue_options.dequeue_mode := DBMS_AQ.REMOVE; -- удаляем после получения
			dequeue_options.wait := :wait_timeout;
			dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
			
			-- Извлечение сообщения
			DBMS_AQ.DEQUEUE(
				queue_name => :queue_name,
				dequeue_options => dequeue_options,
				message_properties => message_properties,
				payload => payload,
				msgid => msgid
			);
			
			-- Преобразуем XMLType в CLOB для возврата
			xml_str := payload.getClobVal();
			:message_handle := msgid;
			:payload := xml_str;
			:success := 1;
		EXCEPTION
			WHEN OTHERS THEN
				IF SQLCODE = -25228 THEN
					-- Очередь пуста - это нормальная ситуация
					:error_code := -25228;
					:success := 0;
				ELSE
					:error_code := SQLCODE;
					:error_msg := SUBSTR(SQLERRM, 1, 4000);
					:success := 0;
				END IF;
		END;
	`

	var messageHandle []byte
	var payload sql.NullString
	var success sql.NullInt64
	var errorCode sql.NullInt64
	var errorMsg sql.NullString

	log.Printf("Попытка извлечения сообщения из очереди %s (consumer: %s, timeout: %d сек)",
		qr.queueName, qr.consumerName, qr.waitTimeout)

	_, err := qr.dbConn.db.ExecContext(qr.dbConn.ctx, plsql,
		sql.Named("consumer_name", qr.consumerName),
		sql.Named("wait_timeout", qr.waitTimeout),
		sql.Named("queue_name", qr.queueName),
		sql.Out{Dest: &messageHandle},
		sql.Out{Dest: &payload},
		sql.Out{Dest: &success},
		sql.Out{Dest: &errorCode},
		sql.Out{Dest: &errorMsg},
	)

	if err != nil {
		log.Printf("Ошибка выполнения PL/SQL блока для DEQUEUE: %v", err)
		return nil, fmt.Errorf("ошибка выполнения PL/SQL блока: %w", err)
	}

	// Проверяем успешность операции
	if success.Valid && success.Int64 == 0 {
		if errorCode.Valid && errorCode.Int64 == -25228 {
			// Очередь пуста - это нормально
			log.Printf("Очередь пуста (код ошибки 25228)")
			return nil, nil
		}
		if errorCode.Valid && errorCode.Int64 != 0 {
			errText := "неизвестная ошибка"
			if errorMsg.Valid {
				errText = errorMsg.String
			}
			log.Printf("Ошибка Oracle при извлечении сообщения (код %d): %s", errorCode.Int64, errText)
			return nil, fmt.Errorf("ошибка Oracle (код %d): %s", errorCode.Int64, errText)
		}
		log.Printf("Неизвестная ошибка при извлечении сообщения (success=0, но errorCode не установлен)")
		return nil, errors.New("неизвестная ошибка при извлечении сообщения")
	}

	// Если payload пуст, возвращаем nil
	if !payload.Valid || payload.String == "" {
		log.Printf("Payload пуст или невалиден")
		return nil, nil
	}

	msg := &QueueMessage{
		MessageID:   fmt.Sprintf("%x", messageHandle),
		XMLPayload:  payload.String,
		RawPayload:  []byte(payload.String),
		DequeueTime: time.Now(),
	}

	log.Printf("Получено сообщение из очереди. MessageID: %s, размер: %d байт", msg.MessageID, len(msg.RawPayload))
	return msg, nil
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
