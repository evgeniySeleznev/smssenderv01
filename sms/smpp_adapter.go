package sms

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	"go.uber.org/zap"

	"oracle-client/logger"
)

// Константы для TON и NPI согласно стандарту SMPP
const (
	TONInternational uint8 = 1 // International number
	TONNational      uint8 = 2 // National number
	NPINational      uint8 = 8 // National numbering plan
	NPIIsdn          uint8 = 1 // ISDN numbering plan
	PriorityHighest  uint8 = 3 // Highest priority
)

// SMPPAdapter представляет адаптер для работы с SMPP протоколом
type SMPPAdapter struct {
	client              *smpp.Transceiver
	config              *SMPPConfig
	mu                  sync.Mutex
	lastAnswerTime      time.Time
	statusChan          <-chan smpp.ConnStatus
	isConnected         bool      // Флаг подключения
	lastBindAttempt     time.Time // Время последней попытки Bind
	consecutiveFailures int       // Количество последовательных неудачных попыток
	lastRebindLogTime   time.Time // Время последнего логирования перед rebind
}

// NewSMPPAdapter создает новый SMPP адаптер
func NewSMPPAdapter(cfg *SMPPConfig) (*SMPPAdapter, error) {
	adapter := &SMPPAdapter{
		config:         cfg,
		lastAnswerTime: time.Now(),
	}

	// Инициализация клиента с параметрами из руководства
	// BindType: Transceiver (BindAsTransceiver)
	// Version: SMPP 3.4 (по умолчанию в библиотеке)
	// NpiType: National (8)
	// TonType: International (1)
	adapter.createClient()

	return adapter, nil
}

// createClient создает новый SMPP клиент
func (a *SMPPAdapter) createClient() {
	addr := fmt.Sprintf("%s:%d", a.config.Host, a.config.Port)

	// EnquireLink по умолчанию отключен (0), как в C# проекте
	// Если EnquireLinkInterval > 0, используем его значение в секундах
	var enquireLink time.Duration
	if a.config.EnquireLinkInterval > 0 {
		enquireLink = time.Duration(a.config.EnquireLinkInterval) * time.Second
	} else {
		enquireLink = 0 // Отключено по умолчанию
	}

	a.client = &smpp.Transceiver{
		Addr:        addr,
		User:        a.config.User,
		Passwd:      a.config.Password,
		EnquireLink: enquireLink,
	}
}

// Bind выполняет подключение к SMPP серверу
func (a *SMPPAdapter) Bind() error {
	a.mu.Lock()
	if logger.Log != nil {
		logger.Log.Debug("Bind(): подключение",
			zap.String("host", a.config.Host),
			zap.Int("port", int(a.config.Port)),
			zap.String("user", a.config.User))
	}

	// Если клиент существует, полностью закрываем его перед переподключением
	if a.client != nil {
		if logger.Log != nil {
			logger.Log.Debug("Bind(): закрытие существующего клиента, если он подключен")
		}
		// Сохраняем ссылку на старый канал для проверки
		oldStatusChan := a.statusChan
		a.unbindInternal()
		a.isConnected = false
		a.statusChan = nil // Очищаем ссылку на канал
		a.mu.Unlock()

		// Даем время на закрытие соединения и завершение горутины
		// Если старая горутина еще работает, она завершится когда канал закроется
		if logger.Log != nil {
			logger.Log.Debug("Bind(): ожидание закрытия старого соединения")
		}
		time.Sleep(500 * time.Millisecond)

		// Проверяем, закрыт ли старый канал (неблокирующая проверка)
		if oldStatusChan != nil {
			select {
			case _, ok := <-oldStatusChan:
				if !ok && logger.Log != nil {
					logger.Log.Debug("Bind(): старый канал статуса закрыт")
				}
			default:
				// Канал еще открыт, но это нормально - он закроется при закрытии клиента
				if logger.Log != nil {
					logger.Log.Debug("Bind(): старый канал еще открыт, продолжаем")
				}
			}
		}

		// Дополнительное время для полной очистки
		time.Sleep(300 * time.Millisecond)
		a.mu.Lock()
	}

	// Применяем экспоненциальную задержку при повторных неудачных попытках
	// чтобы не перегружать сервер слишком частыми запросами
	timeSinceLastAttempt := time.Since(a.lastBindAttempt)
	if a.consecutiveFailures > 0 && timeSinceLastAttempt < time.Duration(a.consecutiveFailures)*time.Second {
		delay := time.Duration(a.consecutiveFailures) * time.Second
		if delay > 30*time.Second {
			delay = 30 * time.Second // Максимальная задержка 30 секунд
		}
		if logger.Log != nil {
			logger.Log.Debug("Bind(): задержка перед повторной попыткой",
				zap.Duration("delay", delay),
				zap.Int("consecutiveFailures", a.consecutiveFailures))
		}
		a.mu.Unlock()
		time.Sleep(delay - timeSinceLastAttempt)
		a.mu.Lock()
	}
	a.lastBindAttempt = time.Now()

	// Всегда создаем новый клиент при переподключении
	if logger.Log != nil {
		logger.Log.Debug("Bind(): создание нового клиента")
	}
	a.createClient()

	// Выполняем Bind и получаем канал статуса
	if logger.Log != nil {
		logger.Log.Debug("Bind(): вызов client.Bind()")
	}
	a.statusChan = a.client.Bind()
	a.mu.Unlock()

	// Ждем первого статуса подключения напрямую из канала библиотеки
	if logger.Log != nil {
		logger.Log.Debug("Bind(): ожидание первого статуса")
	}
	select {
	case status, ok := <-a.statusChan:
		if !ok {
			if logger.Log != nil {
				logger.Log.Warn("Bind(): канал статуса закрыт до получения статуса")
			}
			a.mu.Lock()
			a.consecutiveFailures++
			a.mu.Unlock()
			return fmt.Errorf("канал статуса закрыт")
		}
		if logger.Log != nil {
			logger.Log.Debug("Bind(): получен первый статус",
				zap.String("status", status.Status().String()),
				zap.Error(status.Error()))
		}

		// Проверяем статус подключения
		if status.Status() != smpp.Connected {
			err := status.Error()
			if err != nil && logger.Log != nil {
				logger.Log.Error("Bind(): ошибка подключения", zap.Error(err))
			}
			a.mu.Lock()
			a.consecutiveFailures++
			a.mu.Unlock()
			if err != nil {
				return fmt.Errorf("не удалось подключиться: %w", err)
			}
			return fmt.Errorf("не удалось подключиться: статус %v", status.Status())
		}

		// Подключение успешно - обновляем состояние и запускаем горутину отслеживания
		a.mu.Lock()
		a.isConnected = true
		a.lastAnswerTime = time.Now()
		a.consecutiveFailures = 0 // Сбрасываем счетчик неудач при успешном подключении
		a.mu.Unlock()

		// Запускаем горутину для отслеживания последующих изменений статуса
		go func() {
			if logger.Log != nil {
				logger.Log.Debug("Bind(): горутина отслеживания статуса запущена")
			}
			for status := range a.statusChan {
				if logger.Log != nil {
					logger.Log.Debug("Bind(): получен статус",
						zap.String("status", status.Status().String()),
						zap.Error(status.Error()))
				}
				a.mu.Lock()
				a.isConnected = (status.Status() == smpp.Connected)
				if !a.isConnected && status.Error() != nil {
					if logger.Log != nil {
						logger.Log.Warn("SMPP соединение потеряно", zap.Error(status.Error()))
					}
				}
				if a.isConnected {
					a.lastAnswerTime = time.Now()
				}
				a.mu.Unlock()
			}
			if logger.Log != nil {
				logger.Log.Debug("Bind(): канал статуса закрыт, завершение горутины")
			}
			a.mu.Lock()
			a.isConnected = false
			a.mu.Unlock()
		}()

		if logger.Log != nil {
			logger.Log.Info("Bind(): успешно подключен")
		}
		return nil
	case <-time.After(30 * time.Second):
		if logger.Log != nil {
			logger.Log.Warn("Bind(): таймаут подключения (30 секунд) - статус не получен")
		}
		a.mu.Lock()
		a.consecutiveFailures++
		a.mu.Unlock()
		return fmt.Errorf("таймаут подключения")
	}
}

// unbindInternal выполняет отключение без блокировки
func (a *SMPPAdapter) unbindInternal() {
	if a.client != nil {
		_ = a.client.Close()
	}
}

// Unbind выполняет корректное отключение от SMPP сервера
func (a *SMPPAdapter) Unbind() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.client == nil || !a.isConnected {
		if logger.Log != nil {
			logger.Log.Debug("Unbind() не требуется")
		}
		return
	}

	if logger.Log != nil {
		logger.Log.Info("Unbind() отключение")
	}
	a.unbindInternal()
	if logger.Log != nil {
		logger.Log.Debug("Отключились")
	}
}

// IsConnected проверяет, подключен ли клиент
func (a *SMPPAdapter) IsConnected() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.isConnected && a.client != nil
}

// Rebind выполняет периодическое переподключение, если прошло более указанного времени
// с момента последнего ответа от сервера
func (a *SMPPAdapter) Rebind(rebindIntervalMin uint) bool {
	// Проверяем условия под блокировкой
	a.mu.Lock()

	// Если клиент не существует, нечего переподключать
	if a.client == nil {
		a.mu.Unlock()
		return true
	}

	// Проверяем, прошло ли достаточно времени с последнего ответа
	if rebindIntervalMin == 0 {
		rebindIntervalMin = 60 // По умолчанию 60 минут
	}

	timeSinceLastAnswer := time.Since(a.lastAnswerTime)
	rebindInterval := time.Duration(rebindIntervalMin) * time.Minute

	if timeSinceLastAnswer < rebindInterval {
		// Еще не время для переподключения
		// Логируем только если прошло более 90% интервала и прошло более 5 секунд с последнего логирования
		if timeSinceLastAnswer > rebindInterval*90/100 {
			timeSinceLastLog := time.Since(a.lastRebindLogTime)
			if timeSinceLastLog >= 5*time.Second {
				if logger.Log != nil {
					logger.Log.Debug("Rebind(): проверка",
						zap.Duration("timeSinceLastAnswer", timeSinceLastAnswer),
						zap.Duration("rebindInterval", rebindInterval),
						zap.Duration("remaining", rebindInterval-timeSinceLastAnswer))
				}
				a.lastRebindLogTime = time.Now()
			}
		}
		a.mu.Unlock()
		return true
	}

	if logger.Log != nil {
		logger.Log.Info("Rebind(): выполняется переподключение",
			zap.Duration("timeSinceLastAnswer", timeSinceLastAnswer),
			zap.Duration("rebindInterval", rebindInterval))
	}

	a.mu.Unlock() // Разблокируем перед вызовом Bind, чтобы избежать deadlock

	// Выполняем переподключение (Bind сам заблокирует мьютекс)
	err := a.Bind()

	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("Rebind(): ошибка переподключения", zap.Error(err))
		}
		return false
	}

	if logger.Log != nil {
		logger.Log.Info("Rebind(): успешно переподключен")
	}
	return true
}

// GetLastAnswerTime возвращает время последнего ответа (для тестирования)
func (a *SMPPAdapter) GetLastAnswerTime() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastAnswerTime
}

// SendSMS отправляет SMS через SMPP протокол
func (a *SMPPAdapter) SendSMS(number, text, senderName string) (string, error) {
	// Форматирование номера: добавляем префикс "+7"
	destinationAddress := "+7" + number

	// Определяем значения TON и NPI
	sourceTON := TONInternational
	sourceNPI := NPINational
	destTON := TONInternational
	destNPI := NPINational

	// Применение опциональных параметров адресов из конфигурации
	if a.config.SourceTon != nil {
		sourceTON = uint8(*a.config.SourceTon)
	}
	if a.config.SourceNpi != nil {
		sourceNPI = uint8(*a.config.SourceNpi)
	}
	if a.config.DestTon != nil {
		destTON = uint8(*a.config.DestTon)
	}
	if a.config.DestNpi != nil {
		destNPI = uint8(*a.config.DestNpi)
	}

	// Создание PDU для отправки с параметрами из руководства
	req := &smpp.ShortMessage{
		Src:           senderName,
		Dst:           destinationAddress,
		Text:          pdutext.UCS2(text), // UCS-2 кодировка
		SourceAddrTON: sourceTON,
		SourceAddrNPI: sourceNPI,
		DestAddrTON:   destTON,
		DestAddrNPI:   destNPI,
		PriorityFlag:  PriorityHighest, // Highest priority (3)
	}

	// Отправка с автоматическим переподключением при ошибке
	var smsID string
	var err error

	trySend := func() (string, error) {
		// Проверяем соединение и подключаемся только если не подключены
		if !a.IsConnected() {
			if logger.Log != nil {
				logger.Log.Debug("SendSMS(): соединение не установлено, выполняется подключение")
			}
			if err := a.Bind(); err != nil {
				return "", fmt.Errorf("ошибка подключения: %w", err)
			}
		}

		// Отправка SMS с автоматической сегментацией для длинных сообщений
		// Используем SubmitLongMsg для автоматического разбиения длинных сообщений
		parts, err := a.client.SubmitLongMsg(req)
		if err != nil {
			// Проверяем, является ли ошибка ошибкой соединения
			if isConnectionError(err) {
				if logger.Log != nil {
					logger.Log.Warn("SendSMS(): обнаружена ошибка соединения", zap.Error(err))
				}
				// Помечаем соединение как разорванное
				a.mu.Lock()
				a.isConnected = false
				a.mu.Unlock()
				return "", fmt.Errorf("ошибка соединения: %w", err)
			}
			return "", fmt.Errorf("ошибка отправки сообщения: %w", err)
		}
		if len(parts) == 0 {
			return "", fmt.Errorf("не получен ответ от сервера")
		}
		// Возвращаем MessageID первой части (все части имеют одинаковый MessageID)
		resp := &parts[0]

		// Извлекаем MessageID из ответа
		msgID := resp.RespID()
		if msgID != "" {
			return msgID, nil
		}

		return "", fmt.Errorf("не получен MessageID от сервера")
	}

	// Первая попытка отправки
	smsID, err = trySend()
	if err != nil {
		// Проверяем, является ли это ошибкой соединения
		if isConnectionError(err) {
			if logger.Log != nil {
				logger.Log.Warn("SendSMS(): ошибка соединения, попытка переподключения", zap.Error(err))
			}
			// Попытка переподключения и повторной отправки
			if bindErr := a.Bind(); bindErr != nil {
				return "", fmt.Errorf("ошибка переподключения: %w", bindErr)
			}
			smsID, err = trySend()
			if err != nil {
				return "", fmt.Errorf("ошибка отправки после переподключения: %w", err)
			}
		} else {
			// Это не ошибка соединения - возвращаем ошибку без переподключения
			return "", err
		}
	}

	a.lastAnswerTime = time.Now()
	return smsID, nil
}

// QuerySMSStatus запрашивает статус доставки SMS через команду QUERY_SM
// messageID - ID сообщения, полученный при отправке
// sourceAddr - адрес отправителя (source address)
// sourceTON - тип номера отправителя (TON)
// sourceNPI - план нумерации отправителя (NPI)
// Возвращает статус доставки (message_state) и ошибку
// Статусы: 2 = DELIVERED (доставлено), 3 = EXPIRED (истекло), 5 = UNDELIVERABLE (не доставлено)
func (a *SMPPAdapter) QuerySMSStatus(messageID, sourceAddr string) (int, error) {
	// Проверяем соединение
	if !a.IsConnected() {
		if logger.Log != nil {
			logger.Log.Debug("QuerySMSStatus(): соединение не установлено, выполняется подключение")
		}
		if err := a.Bind(); err != nil {
			return 0, fmt.Errorf("ошибка подключения: %w", err)
		}
	}

	// Определяем значения TON и NPI для source address
	sourceTON := TONInternational
	sourceNPI := NPINational

	// Применение опциональных параметров адресов из конфигурации
	if a.config.SourceTon != nil {
		sourceTON = uint8(*a.config.SourceTon)
	}
	if a.config.SourceNpi != nil {
		sourceNPI = uint8(*a.config.SourceNpi)
	}

	// Отправляем запрос QUERY_SM
	// Сигнатура метода: QuerySM(messageID, sourceAddr string, sourceTON, sourceNPI uint8)
	resp, err := a.client.QuerySM(messageID, sourceAddr, sourceTON, sourceNPI)
	if err != nil {
		// Проверяем, является ли ошибка ошибкой соединения
		if isConnectionError(err) {
			if logger.Log != nil {
				logger.Log.Warn("QuerySMSStatus(): обнаружена ошибка соединения", zap.Error(err))
			}
			// Помечаем соединение как разорванное
			a.mu.Lock()
			a.isConnected = false
			a.mu.Unlock()
			return 0, fmt.Errorf("ошибка соединения: %w", err)
		}
		return 0, fmt.Errorf("ошибка запроса статуса: %w", err)
	}

	// Обновляем время последнего ответа
	a.mu.Lock()
	a.lastAnswerTime = time.Now()
	a.mu.Unlock()

	// Извлекаем message_state из ответа
	// QueryResp имеет поле MsgState типа string
	if resp == nil {
		return 0, fmt.Errorf("пустой ответ от QuerySM")
	}

	// Преобразуем строковый статус в числовой
	// Статусы SMPP: "DELIVRD" (доставлено), "UNDELIV" (не доставлено), "EXPIRED" (истекло)
	// Возвращаем числовые значения: 2 = DELIVERED, 3 = EXPIRED, 5 = UNDELIVERABLE
	msgState := strings.ToUpper(strings.TrimSpace(resp.MsgState))
	var messageState int

	switch msgState {
	case "DELIVRD", "DELIVERED":
		messageState = 2 // DELIVERED
	case "EXPIRED":
		messageState = 3 // EXPIRED
	case "UNDELIV", "UNDELIVERABLE":
		messageState = 5 // UNDELIVERABLE
	case "ENROUTE":
		messageState = 1 // ENROUTE
	case "DELETED":
		messageState = 4 // DELETED
	case "ACCEPTED", "ACCEPTD":
		messageState = 6 // ACCEPTED
	case "UNKNOWN":
		messageState = 7 // UNKNOWN
	case "REJECTED", "REJECTD":
		messageState = 8 // REJECTED
	default:
		// Неизвестный статус - считаем не доставленным
		if logger.Log != nil {
			logger.Log.Warn("QuerySMSStatus(): неизвестный статус",
				zap.String("messageID", messageID),
				zap.String("msgState", msgState))
		}
		messageState = 5 // UNDELIVERABLE по умолчанию
	}

	if logger.Log != nil {
		logger.Log.Debug("QuerySMSStatus(): получен статус",
			zap.String("messageID", messageID),
			zap.String("msgState", msgState),
			zap.Int("messageState", messageState))
	}

	return messageState, nil
}

// isConnectionError проверяет, является ли ошибка ошибкой соединения
// Проверяет как английские, так и русскоязычные ошибки подключения
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Проверяем специфичные ошибки библиотеки go-smpp через errors.Is
	if errors.Is(err, smpp.ErrNotConnected) || errors.Is(err, smpp.ErrNotBound) {
		return true
	}

	// Проверяем строковые паттерны (для ошибок, создаваемых в приложении через fmt.Errorf)
	errStr := strings.ToLower(err.Error())
	connectionErrors := []string{
		// Английские паттерны
		"not connected",
		"not bound",
		"connection",
		"dial tcp",
		"connectex",
		"timeout",
		"broken pipe",
		"connection reset",
		"connection refused",
		"no such host",
		"network is unreachable",
		// Русскоязычные паттерны (для ошибок, обернутых в fmt.Errorf)
		"подключ",
		"соединен",
		"переподключ",
		"smpp",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(errStr, connErr) {
			return true
		}
	}

	return false
}

// Close закрывает соединение
func (a *SMPPAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.client != nil {
		return a.client.Close()
	}
	return nil
}
