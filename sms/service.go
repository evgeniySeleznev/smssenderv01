package sms

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

// SMSMessage представляет распарсенное сообщение из Oracle очереди
type SMSMessage struct {
	TaskID          int64
	PhoneNumber     string
	Message         string
	SenderName      string
	SMPPID          int
	SendingSchedule bool
	DateActiveFrom  *time.Time
}

// SMSResponse представляет результат отправки SMS
type SMSResponse struct {
	TaskID    int64
	MessageID string
	Status    int // 2 = отправлено, 3 = ошибка, 4 = доставлено
	ErrorText string
	SentAt    time.Time
}

// TestPhoneGetter представляет функцию для получения тестового номера телефона
type TestPhoneGetter func() (string, error)

// Service представляет сервис для отправки SMS
type Service struct {
	cfg          *Config
	mu           sync.RWMutex
	lastActivity map[int]time.Time // Последняя активность по каждому SMPP провайдеру
	getTestPhone TestPhoneGetter   // Функция для получения тестового номера (опционально)
}

// NewService создает новый сервис отправки SMS
func NewService(cfg *Config) *Service {
	return &Service{
		cfg:          cfg,
		lastActivity: make(map[int]time.Time),
		getTestPhone: nil, // По умолчанию не установлена
	}
}

// SetTestPhoneGetter устанавливает функцию для получения тестового номера
func (s *Service) SetTestPhoneGetter(getter TestPhoneGetter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getTestPhone = getter
}

// ProcessSMS обрабатывает распарсенное сообщение из Oracle и отправляет SMS
func (s *Service) ProcessSMS(msg SMSMessage) (*SMSResponse, error) {
	// Получение адаптера по SMPPID
	s.mu.RLock()
	smppCfg, ok := s.cfg.SMPP[msg.SMPPID]
	s.mu.RUnlock()

	if !ok {
		errText := fmt.Sprintf("SMS не отправлено: нет данных для указанного SMPP (ID=%d)", msg.SMPPID)
		log.Printf("Ошибка: %s", errText)
		return &SMSResponse{
			TaskID:    msg.TaskID,
			MessageID: "",
			Status:    3, // ошибка
			ErrorText: errText,
			SentAt:    time.Now(),
		}, nil
	}

	// Проверка расписания (если требуется)
	if msg.SendingSchedule {
		if err := s.checkSchedule(msg.DateActiveFrom); err != nil {
			errText := err.Error()
			log.Printf("Ошибка расписания: %s", errText)
			return &SMSResponse{
				TaskID:    msg.TaskID,
				MessageID: "",
				Status:    3, // ошибка
				ErrorText: errText,
				SentAt:    time.Now(),
			}, nil
		}
	}

	// Режим Debug - замена номера на тестовый
	phoneNumber := msg.PhoneNumber
	if s.cfg.Mode.Debug {
		s.mu.RLock()
		getTestPhone := s.getTestPhone
		s.mu.RUnlock()

		if getTestPhone != nil {
			testNumber, err := getTestPhone()
			if err != nil {
				errText := fmt.Sprintf("Ошибка получения тестового номера: %v", err)
				log.Printf("Ошибка: %s", errText)
				return &SMSResponse{
					TaskID:    msg.TaskID,
					MessageID: "",
					Status:    3, // ошибка
					ErrorText: errText,
					SentAt:    time.Now(),
				}, nil
			}
			log.Printf("Режим Debug: номер %s заменен на тестовый номер %s", phoneNumber, testNumber)
			phoneNumber = testNumber
		} else {
			log.Printf("Режим Debug: функция получения тестового номера не установлена, используется исходный номер %s", phoneNumber)
		}
	}

	// Отправка SMS
	messageID, err := s.sendSMS(msg, smppCfg, phoneNumber)
	if err != nil {
		errText := fmt.Sprintf("Ошибка отправки абоненту: %v", err)
		log.Printf("Ошибка отправки SMS: %s", errText)
		return &SMSResponse{
			TaskID:    msg.TaskID,
			MessageID: "",
			Status:    3, // ошибка
			ErrorText: errText,
			SentAt:    time.Now(),
		}, nil
	}

	// Успешная отправка
	log.Printf("SMS успешно отправлено: TaskID=%d, MessageID=%s, Phone=%s, Sender=%s",
		msg.TaskID, messageID, phoneNumber, msg.SenderName)

	return &SMSResponse{
		TaskID:    msg.TaskID,
		MessageID: messageID,
		Status:    2, // отправлено успешно
		ErrorText: "",
		SentAt:    time.Now(),
	}, nil
}

// sendSMS отправляет SMS через SMPP (или логирует в режиме Silent)
func (s *Service) sendSMS(msg SMSMessage, smppCfg *SMPPConfig, phoneNumber string) (string, error) {
	// Режим Silent - не отправляем реально, только логируем
	if s.cfg.Mode.Silent {
		log.Printf("[SILENT MODE] SMS не отправляется реально. Параметры:")
		log.Printf("  TaskID: %d", msg.TaskID)
		log.Printf("  Phone: +7%s", phoneNumber)
		log.Printf("  Message: %s", msg.Message)
		log.Printf("  Sender: %s", msg.SenderName)
		log.Printf("  SMPP ID: %d", msg.SMPPID)
		log.Printf("  SMPP Host: %s:%d", smppCfg.Host, smppCfg.Port)
		log.Printf("  SMPP User: %s", smppCfg.User)

		// Генерируем фиктивный MessageID для логирования
		messageID := fmt.Sprintf("silent-%d-%d", msg.TaskID, time.Now().Unix())
		return messageID, nil
	}

	// Здесь будет реальная отправка через SMPP библиотеку
	// Пока не реализовано, так как режим Silent включен по умолчанию
	log.Printf("[REAL MODE] Отправка SMS через SMPP (пока не реализовано)")
	log.Printf("  TaskID: %d", msg.TaskID)
	log.Printf("  Phone: +7%s", phoneNumber)
	log.Printf("  Message: %s", msg.Message)
	log.Printf("  Sender: %s", msg.SenderName)
	log.Printf("  SMPP Host: %s:%d", smppCfg.Host, smppCfg.Port)

	// TODO: Реальная отправка через SMPP библиотеку
	// messageID, err := smppAdapter.SendSMS(phoneNumber, msg.Message, msg.SenderName)
	// return messageID, err

	return "", fmt.Errorf("реальная отправка SMS пока не реализована (используйте Silent mode)")
}

// checkSchedule проверяет, попадает ли время в рабочее расписание
func (s *Service) checkSchedule(dateActiveFrom *time.Time) error {
	now := time.Now()
	checkTime := now

	// Если указана date_active_from, используем её
	if dateActiveFrom != nil {
		checkTime = *dateActiveFrom
	}

	// Получаем время начала и конца рабочего дня для даты проверки
	checkDate := time.Date(checkTime.Year(), checkTime.Month(), checkTime.Day(), 0, 0, 0, 0, checkTime.Location())

	// Извлекаем часы и минуты из TimeStart и TimeEnd
	timeStartHour := s.cfg.Schedule.TimeStart.Hour()
	timeStartMin := s.cfg.Schedule.TimeStart.Minute()
	timeEndHour := s.cfg.Schedule.TimeEnd.Hour()
	timeEndMin := s.cfg.Schedule.TimeEnd.Minute()

	// Формируем полное время начала и конца рабочего дня
	timeStart := checkDate.Add(time.Duration(timeStartHour)*time.Hour + time.Duration(timeStartMin)*time.Minute)
	timeEnd := checkDate.Add(time.Duration(timeEndHour)*time.Hour + time.Duration(timeEndMin)*time.Minute)

	// Проверка попадания в интервал
	if checkTime.Before(timeStart) || checkTime.After(timeEnd) {
		return fmt.Errorf("попытка отправки SMS за пределами расписания [%s..%s]",
			timeStart.Format("15:04"), timeEnd.Format("15:04"))
	}

	return nil
}

// ParseSMSMessage преобразует map из ParseXMLMessage в SMSMessage
func ParseSMSMessage(parsed map[string]interface{}) (*SMSMessage, error) {
	msg := &SMSMessage{}

	// Парсинг sms_task_id
	if taskIDStr, ok := parsed["sms_task_id"].(string); ok && taskIDStr != "" {
		taskID, err := strconv.ParseInt(taskIDStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("неверный формат sms_task_id: %w", err)
		}
		msg.TaskID = taskID
	}

	// Парсинг phone_number
	if phoneNumber, ok := parsed["phone_number"].(string); ok {
		msg.PhoneNumber = phoneNumber
	}

	// Парсинг message
	if message, ok := parsed["message"].(string); ok {
		msg.Message = message
	}

	// Парсинг sender_name
	if senderName, ok := parsed["sender_name"].(string); ok {
		msg.SenderName = senderName
	}

	// Парсинг smpp_id
	if smppIDStr, ok := parsed["smpp_id"].(string); ok && smppIDStr != "" {
		smppID, err := strconv.Atoi(smppIDStr)
		if err != nil {
			return nil, fmt.Errorf("неверный формат smpp_id: %w", err)
		}
		msg.SMPPID = smppID
	}

	// Парсинг sending_schedule
	if sendingScheduleStr, ok := parsed["sending_schedule"].(string); ok {
		msg.SendingSchedule = (sendingScheduleStr == "1")
	}

	// Парсинг date_active_from (опционально)
	if dateActiveFromStr, ok := parsed["date_active_from"].(string); ok && dateActiveFromStr != "" {
		// Пробуем различные форматы даты
		dateFormats := []string{
			"2006-01-02T15:04:05", // ISO с T
			"2006-01-02 15:04:05", // ISO с пробелом и секундами
			"2006-01-02 15:04",    // ISO с пробелом без секунд
			"02.01.2006 15:04:05", // DD.MM.YYYY с секундами
			"02.01.2006 15:04",    // DD.MM.YYYY без секунд
			"02.01.2006T15:04:05", // DD.MM.YYYY с T и секундами
			"02.01.2006T15:04",    // DD.MM.YYYY с T без секунд
		}

		var dateActiveFrom time.Time
		var err error
		parsed := false

		for _, format := range dateFormats {
			dateActiveFrom, err = time.Parse(format, dateActiveFromStr)
			if err == nil {
				msg.DateActiveFrom = &dateActiveFrom
				parsed = true
				break
			}
		}

		if !parsed {
			log.Printf("Предупреждение: не удалось распарсить date_active_from: %s, ошибка: %v", dateActiveFromStr, err)
		}
	}

	return msg, nil
}
