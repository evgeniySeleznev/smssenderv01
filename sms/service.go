package sms

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
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
	Status    int // 2 = отправлено, 3 = ошибка/не доставлено, 4 = доставлено
	ErrorText string
	SentAt    time.Time
}

// TestPhoneGetter представляет функцию для получения тестового номера телефона
type TestPhoneGetter func() (string, error)

// SaveResponseCallback - callback для сохранения результата отправки SMS в БД
type SaveResponseCallback func(response *SMSResponse)

// Service представляет сервис для отправки SMS
type Service struct {
	cfg            *Config
	mu             sync.RWMutex
	lastActivity   map[int]time.Time    // Последняя активность по каждому SMPP провайдеру
	getTestPhone   TestPhoneGetter      // Функция для получения тестового номера (опционально)
	adapters       map[int]*SMPPAdapter // Кэш адаптеров по SMPP ID
	initialized    bool                 // Флаг инициализации адаптеров
	rebindTicker   *time.Ticker         // Таймер для периодического переподключения
	rebindStop     chan struct{}        // Канал для остановки переподключения
	rebindWg       sync.WaitGroup       // WaitGroup для ожидания завершения горутины переподключения
	retryQueue     []*RetryMessage      // Очередь сообщений для повторной отправки (неограниченная)
	retryQueueMu   sync.Mutex           // Мьютекс для защиты очереди повторных попыток
	retryWg        sync.WaitGroup       // WaitGroup для ожидания завершения горутины повторных попыток
	retryStop      chan struct{}        // Канал для остановки механизма повторных попыток
	retryStarted      bool                 // Флаг запуска механизма повторных попыток
	scheduledQueue    *ScheduledQueue      // Очередь отложенных сообщений (для schedule=1 вне окна)
	saveCallback      SaveResponseCallback // Callback для сохранения результата в БД
	statusChecker     *StatusChecker       // Компонент для проверки статуса доставки SMS
}

// NewService создает новый сервис отправки SMS
func NewService(cfg *Config) *Service {
	s := &Service{
		cfg:          cfg,
		lastActivity: make(map[int]time.Time),
		getTestPhone: nil, // По умолчанию не установлена
		adapters:     make(map[int]*SMPPAdapter),
		initialized:  false,
		rebindStop:   make(chan struct{}),
		retryQueue:   make([]*RetryMessage, 0), // Неограниченная очередь для повторных попыток
		retryStop:    make(chan struct{}),
	}
	// Создаем очередь отложенных сообщений
	s.scheduledQueue = NewScheduledQueue(cfg, s)
	// Создаем компонент для проверки статуса доставки
	s.statusChecker = NewStatusChecker(s)
	return s
}

// SetSaveCallback устанавливает callback для сохранения результата SMS в БД
// Используется очередью отложенных сообщений для сохранения результатов
func (s *Service) SetSaveCallback(callback SaveResponseCallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCallback = callback
}

// GetSaveCallback возвращает callback для сохранения результата SMS в БД
func (s *Service) GetSaveCallback() SaveResponseCallback {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.saveCallback
}

// InitializeAdapters инициализирует все SMPP адаптеры и устанавливает соединения заранее
func (s *Service) InitializeAdapters() error {
	s.mu.Lock()

	if s.initialized {
		s.mu.Unlock()
		return nil
	}

	if logger.Log != nil {
		logger.Log.Info("Инициализация SMPP адаптеров...")
	}

	// Создаем адаптеры для всех настроенных SMPP провайдеров
	for smppID, smppCfg := range s.cfg.SMPP {
		if logger.Log != nil {
			logger.Log.Info("Инициализация SMPP адаптера",
				zap.Int("smppID", smppID),
				zap.String("host", smppCfg.Host),
				zap.Int("port", int(smppCfg.Port)),
				zap.String("user", smppCfg.User))
		}

		adapter, err := NewSMPPAdapter(smppCfg)
		if err != nil {
			if logger.Log != nil {
				logger.Log.Error("Ошибка создания адаптера для SMPP", zap.Int("smppID", smppID), zap.Error(err))
			}
			continue
		}

		s.adapters[smppID] = adapter

		// Устанавливаем соединение заранее (только в реальном режиме)
		// Разблокируем перед вызовом Bind, чтобы избежать deadlock
		s.mu.Unlock()
		if !s.cfg.Mode.Silent {
			if logger.Log != nil {
				logger.Log.Info("Предварительное подключение SMPP", zap.Int("smppID", smppID))
			}
			if err := adapter.Bind(); err != nil {
				if logger.Log != nil {
					logger.Log.Warn("Не удалось подключиться к SMPP (будет попытка при первой отправке)",
						zap.Int("smppID", smppID),
						zap.Error(err))
				}
				// Не возвращаем ошибку - соединение установится при первой отправке
			} else {
				if logger.Log != nil {
					logger.Log.Info("SMPP адаптер успешно подключен", zap.Int("smppID", smppID))
				}
			}
		}
		s.mu.Lock() // Блокируем обратно для следующей итерации или завершения
	}

	s.initialized = true
	if logger.Log != nil {
		logger.Log.Info("Инициализация SMPP адаптеров завершена")
	}
	s.mu.Unlock() // Разблокируем перед вызовом StartPeriodicRebind, чтобы избежать deadlock

	// Запускаем механизм периодического переподключения (после разблокировки)
	s.StartPeriodicRebind()

	// Запускаем механизм повторных попыток отправки SMS
	s.StartRetryWorker()

	return nil
}

// StartScheduledQueue запускает горутину обработки отложенных сообщений
// ctx - контекст для graceful shutdown
func (s *Service) StartScheduledQueue(ctx context.Context) {
	if s.scheduledQueue != nil {
		s.scheduledQueue.Start(ctx)
	}
}

// StopScheduledQueue останавливает горутину обработки отложенных сообщений
func (s *Service) StopScheduledQueue() {
	if s.scheduledQueue != nil {
		s.scheduledQueue.Stop()
	}
}

// GetScheduledQueueSize возвращает количество сообщений в очереди отложенных
func (s *Service) GetScheduledQueueSize() int {
	if s.scheduledQueue != nil {
		return s.scheduledQueue.GetQueueSize()
	}
	return 0
}

// SetShutdownTimeout устанавливает таймаут для graceful shutdown
// Влияет на очередь отложенных сообщений и другие операции завершения
func (s *Service) SetShutdownTimeout(timeout time.Duration) {
	if s.scheduledQueue != nil {
		s.scheduledQueue.SetShutdownTimeout(timeout)
	}
}

// StartPeriodicRebind запускает горутину для периодического переподключения SMPP адаптеров
func (s *Service) StartPeriodicRebind() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Если уже запущено, не запускаем повторно
	if s.rebindTicker != nil {
		return
	}

	// Используем RebindSMPPMin из конфига, по умолчанию 60 минут
	rebindIntervalMin := s.cfg.Schedule.RebindSMPPMin
	if rebindIntervalMin == 0 {
		rebindIntervalMin = 60 // По умолчанию 60 минут
	}

	// Проверяем состояние соединения каждые 500 мс (как в C# проекте)
	// Это позволяет быстро обнаруживать разрывы соединения даже когда нет SMS для отправки
	checkInterval := 500 * time.Millisecond
	s.rebindTicker = time.NewTicker(checkInterval)
	s.rebindWg.Add(1)

	if logger.Log != nil {
		logger.Log.Info("Запущен механизм периодического переподключения SMPP",
			zap.Duration("checkInterval", checkInterval),
			zap.Uint("rebindIntervalMin", rebindIntervalMin))
	}

	go func() {
		defer s.rebindWg.Done()
		defer s.rebindTicker.Stop()

		for {
			select {
			case <-s.rebindTicker.C:
				// Время проверки - проверяем все адаптеры
				s.mu.RLock()
				adaptersCopy := make(map[int]*SMPPAdapter)
				for id, adapter := range s.adapters {
					adaptersCopy[id] = adapter
				}
				s.mu.RUnlock()

				// Проверяем и переподключаем каждый адаптер
				// Используем rebindIntervalMin из замыкания (вычислено один раз при запуске)
				for smppID, adapter := range adaptersCopy {
					if adapter != nil {
						// Сначала проверяем реальное состояние соединения
						// Если соединение разорвано, переподключаемся сразу
						if !adapter.IsConnected() {
							if logger.Log != nil {
								logger.Log.Warn("Обнаружено разорванное соединение для SMPP, выполняется переподключение",
									zap.Int("smppID", smppID))
							}
							if err := adapter.Bind(); err != nil {
								if logger.Log != nil {
									logger.Log.Error("Ошибка переподключения для SMPP", zap.Int("smppID", smppID), zap.Error(err))
								}
							}
						} else {
							// Соединение активно - проверяем время с последнего ответа для периодического Rebind
							if !adapter.Rebind(rebindIntervalMin) {
								if logger.Log != nil {
									logger.Log.Warn("Не удалось выполнить Rebind для SMPP", zap.Int("smppID", smppID))
								}
							}
						}
					}
				}

			case <-s.rebindStop:
				if logger.Log != nil {
					logger.Log.Info("Остановка механизма периодического переподключения SMPP")
				}
				return
			}
		}
	}()
}

// StopPeriodicRebind останавливает механизм периодического переподключения
func (s *Service) StopPeriodicRebind() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rebindTicker != nil {
		close(s.rebindStop)
		s.rebindTicker.Stop()
		s.rebindTicker = nil
		s.rebindWg.Wait()
		// Создаем новый канал для следующего запуска
		s.rebindStop = make(chan struct{})
	}
}

// startStatusCheck запускает горутину для проверки статуса доставки SMS через 5 минут
func (s *Service) startStatusCheck(taskID int64, messageID, senderName string, smppID int) {
	if s.statusChecker != nil {
		s.statusChecker.StartStatusCheck(taskID, messageID, senderName, smppID)
	}
}

// Close закрывает все SMPP адаптеры и освобождает ресурсы
// Перед вызовом Close() рекомендуется вызвать StopPeriodicRebind() и StopRetryWorker() для остановки механизмов
func (s *Service) Close() error {
	// Останавливаем механизм повторных попыток
	s.StopRetryWorker()

	// Останавливаем очередь отложенных сообщений
	s.StopScheduledQueue()

	// Ждем завершения всех горутин проверки статуса
	if s.statusChecker != nil {
		if logger.Log != nil {
			logger.Log.Info("Ожидание завершения проверки статусов доставки...")
		}
		s.statusChecker.Wait()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Закрываем все адаптеры
	var lastErr error
	for id, adapter := range s.adapters {
		if adapter != nil {
			if err := adapter.Close(); err != nil {
				if logger.Log != nil {
					logger.Log.Error("Ошибка закрытия SMPP адаптера", zap.Int("smppID", id), zap.Error(err))
				}
				lastErr = err
			} else {
				if logger.Log != nil {
					logger.Log.Info("SMPP адаптер закрыт", zap.Int("smppID", id))
				}
			}
		}
	}

	return lastErr
}

// SetTestPhoneGetter устанавливает функцию для получения тестового номера
func (s *Service) SetTestPhoneGetter(getter TestPhoneGetter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getTestPhone = getter
}

// ProcessSMS обрабатывает распарсенное сообщение из Oracle и отправляет SMS
// Принимает контекст для возможности отмены операций при graceful shutdown
// Если контекст отменен, SMS не отправляется и возвращается ошибка
func (s *Service) ProcessSMS(ctx context.Context, msg SMSMessage) (*SMSResponse, error) {
	// НЕ проверяем контекст здесь - проверка происходит в вызывающем коде
	// При graceful shutdown вызывающий код заменяет отмененный контекст на shutdownCtx с таймаутом
	// Это позволяет завершить критически важные операции (отправка SMS и сохранение в БД)

	// Получение адаптера по SMPPID
	s.mu.RLock()
	smppCfg, ok := s.cfg.SMPP[msg.SMPPID]
	s.mu.RUnlock()

	if !ok {
		errText := fmt.Sprintf("SMS не отправлено: нет данных для указанного SMPP (ID=%d)", msg.SMPPID)
		if logger.Log != nil {
			logger.Log.Error("Ошибка: нет данных для указанного SMPP", zap.Int("smppID", msg.SMPPID))
		}
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
			// Вместо возврата ошибки добавляем сообщение в очередь отложенных
			if s.scheduledQueue != nil {
				if logger.Log != nil {
					logger.Log.Info("SMS добавлено в очередь отложенных (вне окна расписания)",
						zap.Int64("taskID", msg.TaskID),
						zap.String("reason", err.Error()))
				}
				s.scheduledQueue.Enqueue(msg, err.Error())
				// Возвращаем nil, nil чтобы не сохранять в БД - сообщение будет обработано позже
				return nil, nil
			}
			// Если очередь не инициализирована - возвращаем ошибку как раньше
			errText := err.Error()
			if logger.Log != nil {
				logger.Log.Error("Ошибка расписания", zap.Error(err))
			}
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
				if logger.Log != nil {
					logger.Log.Error(errText)
				}
				return &SMSResponse{
					TaskID:    msg.TaskID,
					MessageID: "",
					Status:    3, // ошибка
					ErrorText: errText,
					SentAt:    time.Now(),
				}, nil
			}

			if strings.TrimSpace(testNumber) == "" {
				errText := "Режим Debug: тестовый номер отсутствует, SMS не отправляется"
				if logger.Log != nil {
					logger.Log.Warn(errText)
				}
				return &SMSResponse{
					TaskID:    msg.TaskID,
					MessageID: "",
					Status:    3, // ошибка
					ErrorText: errText,
					SentAt:    time.Now(),
				}, nil
			}

			if logger.Log != nil {
				logger.Log.Debug("Режим Debug: номер заменен на тестовый",
					zap.String("original", phoneNumber),
					zap.String("test", testNumber))
			}
			phoneNumber = testNumber
		} else {
			if logger.Log != nil {
				logger.Log.Debug("Режим Debug: функция получения тестового номера не установлена, используется исходный номер",
					zap.String("phone", phoneNumber))
			}
		}
	}

	// НЕ проверяем контекст здесь - проверка происходит в вызывающем коде
	// При graceful shutdown вызывающий код заменяет отмененный контекст на shutdownCtx с таймаутом
	// Это позволяет завершить критически важные операции (отправка SMS и сохранение в БД)

	// Отправка SMS
	messageID, err := s.sendSMS(msg, smppCfg, phoneNumber)
	if err != nil {
		// Проверяем, является ли это ошибкой SMPP провайдера
		if s.isSMPPProviderError(err) {
			if logger.Log != nil {
				logger.Log.Warn("Ошибка SMPP провайдера при отправке SMS, отправка в очередь повторных попыток",
					zap.Int64("taskID", msg.TaskID),
					zap.Error(err))
			}
			// Отправляем в очередь повторных попыток
			s.enqueueForRetry(msg, err)
			// Возвращаем статус ошибки, так как сообщение будет отправлено повторно (до 10 попыток)
			return &SMSResponse{
				TaskID:    msg.TaskID,
				MessageID: "",
				Status:    3, // Ошибка (будет отправлено повторно до 10 раз)
				ErrorText: fmt.Sprintf("Ошибка отправки, будет повторная попытка: %v", err),
				SentAt:    time.Now(),
			}, nil
		}
		// Это не ошибка SMPP провайдера - возвращаем ошибку без повторных попыток
		errText := fmt.Sprintf("Ошибка отправки абоненту: %v", err)
		if logger.Log != nil {
			logger.Log.Error("Ошибка отправки SMS", zap.String("error", errText))
		}
		return &SMSResponse{
			TaskID:    msg.TaskID,
			MessageID: "",
			Status:    3, // ошибка
			ErrorText: errText,
			SentAt:    time.Now(),
		}, nil
	}

	// Успешная отправка
	if logger.Log != nil {
		logger.Log.Info("SMS успешно отправлено",
			zap.Int64("taskID", msg.TaskID),
			zap.String("messageID", messageID),
			zap.String("phone", phoneNumber),
			zap.String("sender", msg.SenderName))
	}

	// Запускаем горутину для проверки статуса доставки через 5 минут
	s.startStatusCheck(msg.TaskID, messageID, msg.SenderName, msg.SMPPID)

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
		if logger.Log != nil {
			logger.Log.Info("[SILENT MODE] SMS не отправляется реально",
				zap.Int64("taskID", msg.TaskID),
				zap.String("phone", "+7"+phoneNumber),
				zap.String("message", msg.Message),
				zap.String("sender", msg.SenderName),
				zap.Int("smppID", msg.SMPPID),
				zap.String("smppHost", fmt.Sprintf("%s:%d", smppCfg.Host, smppCfg.Port)),
				zap.String("smppUser", smppCfg.User))
		}

		// Генерируем фиктивный MessageID для логирования
		messageID := fmt.Sprintf("silent-%d-%d", msg.TaskID, time.Now().Unix())
		return messageID, nil
	}

	// Реальная отправка через SMPP библиотеку
	if logger.Log != nil {
		logger.Log.Debug("[REAL MODE] Отправка SMS через SMPP",
			zap.Int64("taskID", msg.TaskID),
			zap.String("phone", "+7"+phoneNumber),
			zap.String("message", msg.Message),
			zap.String("sender", msg.SenderName),
			zap.String("smppHost", fmt.Sprintf("%s:%d", smppCfg.Host, smppCfg.Port)))
	}

	// Получаем или создаем адаптер для данного SMPP провайдера
	adapter, err := s.getOrCreateAdapter(msg.SMPPID, smppCfg)
	if err != nil {
		return "", fmt.Errorf("ошибка создания SMPP адаптера: %w", err)
	}

	// Отправка SMS через адаптер
	messageID, err := adapter.SendSMS(phoneNumber, msg.Message, msg.SenderName)
	if err != nil {
		return "", fmt.Errorf("ошибка отправки SMS: %w", err)
	}

	if logger.Log != nil {
		logger.Log.Debug("SMS отправлено",
			zap.String("sender", msg.SenderName),
			zap.String("number", phoneNumber),
			zap.String("text", msg.Message),
			zap.String("messageID", messageID))
	}

	return messageID, nil
}

// EnsureSMPPConnectivity убеждается, что хотя бы один SMPP адаптер подключен или может переподключиться.
// Используется до чтения очереди, чтобы не извлекать сообщения, если отправка заведомо невозможна.
func (s *Service) EnsureSMPPConnectivity() bool {
	s.mu.RLock()
	adaptersCopy := make(map[int]*SMPPAdapter, len(s.adapters))
	for id, adapter := range s.adapters {
		adaptersCopy[id] = adapter
	}
	s.mu.RUnlock()

	if len(adaptersCopy) == 0 {
		if logger.Log != nil {
			logger.Log.Warn("Проверка подключения к SMPP: нет инициализированных SMPP адаптеров")
		}
		return false
	}

	for smppID, adapter := range adaptersCopy {
		if adapter == nil {
			continue
		}
		if adapter.IsConnected() {
			return true
		}

		if logger.Log != nil {
			logger.Log.Debug("Проверка подключения к SMPP: SMPP не подключен, попытка Bind", zap.Int("smppID", smppID))
		}
		if err := adapter.Bind(); err != nil {
			if logger.Log != nil {
				logger.Log.Warn("Проверка подключения к SMPP: не удалось подключиться", zap.Int("smppID", smppID), zap.Error(err))
			}
			continue
		}

		return true
	}

	if logger.Log != nil {
		logger.Log.Warn("Проверка подключения к SMPP: ни один SMPP адаптер не доступен")
	}
	return false
}

// getOrCreateAdapter получает существующий адаптер или создает новый для указанного SMPP ID
func (s *Service) getOrCreateAdapter(smppID int, smppCfg *SMPPConfig) (*SMPPAdapter, error) {
	s.mu.RLock()
	adapter, ok := s.adapters[smppID]
	s.mu.RUnlock()

	if ok {
		return adapter, nil
	}

	// Адаптер не найден - создаем новый (это не должно происходить после инициализации)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Двойная проверка на случай параллельного доступа
	if adapter, ok := s.adapters[smppID]; ok {
		return adapter, nil
	}

	if logger.Log != nil {
		logger.Log.Info("Создание нового адаптера для SMPP (не был инициализирован заранее)", zap.Int("smppID", smppID))
	}
	adapter, err := NewSMPPAdapter(smppCfg)
	if err != nil {
		return nil, err
	}

	s.adapters[smppID] = adapter
	return adapter, nil
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
			if logger.Log != nil {
				logger.Log.Warn("Не удалось распарсить date_active_from", zap.String("dateActiveFrom", dateActiveFromStr), zap.Error(err))
			}
		}
	}

	return msg, nil
}
