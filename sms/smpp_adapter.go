package sms

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutlv"
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

// SMPP MessageState значения (согласно спецификации SMPP 3.4)
const (
	SMPPMessageStateEnroute       = 1 // Сообщение в пути
	SMPPMessageStateDelivered     = 2 // Доставлено абоненту
	SMPPMessageStateExpired       = 3 // Истек срок доставки
	SMPPMessageStateDeleted       = 4 // Удалено
	SMPPMessageStateUndeliverable = 5 // Не доставлено
	SMPPMessageStateAccepted      = 6 // Принято
	SMPPMessageStateUnknown       = 7 // Неизвестно
	SMPPMessageStateRejected      = 8 // Отклонено
)

// Статусы для сохранения в БД
const (
	StatusSentSuccessfully   = 2 // Успешно отправлено
	StatusErrorOrUndelivered = 3 // Ошибка отправки или не доставлено
	StatusDelivered          = 4 // Доставлено абоненту
)

// DeliveryReceipt представляет delivery receipt от SMPP провайдера
type DeliveryReceipt struct {
	ReceiptedMessageID string    // ID сообщения, для которого пришел receipt
	MessageState       int       // Статус доставки (SMPP MessageState)
	SequenceNumber     uint32    // Номер последовательности PDU
	ErrorText          string    // Текст ошибки (если есть)
	ReceivedAt         time.Time // Время получения receipt
	StatusID           int       // Статус для сохранения в БД (2, 3 или 4)
}

// DeliveryReceiptCallback - callback для обработки одиночного delivery receipt
type DeliveryReceiptCallback func(receipt *DeliveryReceipt)

// BatchDeliveryReceiptCallback - callback для batch-обработки delivery receipts
type BatchDeliveryReceiptCallback func(receipts []*DeliveryReceipt) int

// Константы для буферной очереди delivery receipts
const (
	deliveryReceiptQueueSize    = 2000            // Размер буфера очереди
	deliveryReceiptBatchSize    = 50              // Размер batch для сохранения
	deliveryReceiptBatchTimeout = 1 * time.Second // Таймаут для сбора batch
)

// SMPPAdapter представляет адаптер для работы с SMPP протоколом
type SMPPAdapter struct {
	client                       *smpp.Transceiver
	config                       *SMPPConfig
	mu                           sync.Mutex
	lastAnswerTime               time.Time
	statusChan                   <-chan smpp.ConnStatus
	isConnected                  bool                         // Флаг подключения
	lastBindAttempt              time.Time                    // Время последней попытки Bind
	consecutiveFailures          int                          // Количество последовательных неудачных попыток
	lastRebindLogTime            time.Time                    // Время последнего логирования перед rebind
	deliveryReceiptCallback      DeliveryReceiptCallback      // Callback для обработки одиночного receipt (deprecated)
	batchDeliveryReceiptCallback BatchDeliveryReceiptCallback // Callback для batch-обработки receipts
	deliveryReceiptWg            sync.WaitGroup               // WaitGroup для ожидания завершения обработки receipts
	deliveryReceiptWgMu          sync.Mutex                   // Мьютекс для защиты WaitGroup от повторного использования
	deliveryReceiptWaitRunning   bool                         // Флаг, что Wait() уже запущен
	deliveryReceiptQueue         chan *DeliveryReceipt        // Буферная очередь для delivery receipts
	receiptWorkerStop            chan struct{}                // Канал для остановки воркера очереди
	receiptWorkerRunning         bool                         // Флаг работы воркера
}

// NewSMPPAdapter создает новый SMPP адаптер
func NewSMPPAdapter(cfg *SMPPConfig) (*SMPPAdapter, error) {
	adapter := &SMPPAdapter{
		config:               cfg,
		lastAnswerTime:       time.Now(),
		deliveryReceiptQueue: make(chan *DeliveryReceipt, deliveryReceiptQueueSize),
		receiptWorkerStop:    make(chan struct{}),
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
		Handler:     a.handleIncomingPDU, // Обработчик входящих PDU (delivery receipts)
	}
}

// SetDeliveryReceiptCallback устанавливает callback для обработки одиночных delivery receipts
// Deprecated: используйте SetBatchDeliveryReceiptCallback для лучшей производительности
func (a *SMPPAdapter) SetDeliveryReceiptCallback(callback DeliveryReceiptCallback) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deliveryReceiptCallback = callback
}

// SetBatchDeliveryReceiptCallback устанавливает callback для batch-обработки delivery receipts
// Batch формируется по 50 receipts или каждые 2 секунды (что раньше)
func (a *SMPPAdapter) SetBatchDeliveryReceiptCallback(callback BatchDeliveryReceiptCallback) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batchDeliveryReceiptCallback = callback
}

// StartDeliveryReceiptWorker запускает горутину для batch-обработки очереди delivery receipts
// Собирает batch по 50 receipts или каждые 2 секунды (что раньше)
func (a *SMPPAdapter) StartDeliveryReceiptWorker() {
	a.mu.Lock()
	if a.receiptWorkerRunning {
		a.mu.Unlock()
		return
	}
	a.receiptWorkerRunning = true
	a.mu.Unlock()

	if logger.Log != nil {
		logger.Log.Info("Запуск воркера обработки delivery receipts (batch mode)",
			zap.Int("queueSize", deliveryReceiptQueueSize),
			zap.Int("batchSize", deliveryReceiptBatchSize),
			zap.Duration("batchTimeout", deliveryReceiptBatchTimeout))
	}

	go func() {
		batch := make([]*DeliveryReceipt, 0, deliveryReceiptBatchSize)
		ticker := time.NewTicker(deliveryReceiptBatchTimeout)
		defer ticker.Stop()

		for {
			select {
			case receipt := <-a.deliveryReceiptQueue:
				if receipt == nil {
					continue
				}
				batch = append(batch, receipt)

				// Если набрали полный batch - сохраняем
				if len(batch) >= deliveryReceiptBatchSize {
					a.processBatch(batch)
					batch = make([]*DeliveryReceipt, 0, deliveryReceiptBatchSize)
					ticker.Reset(deliveryReceiptBatchTimeout)
				}

			case <-ticker.C:
				// Таймаут - сохраняем что накопилось
				if len(batch) > 0 {
					a.processBatch(batch)
					batch = make([]*DeliveryReceipt, 0, deliveryReceiptBatchSize)
				}

			case <-a.receiptWorkerStop:
				if logger.Log != nil {
					logger.Log.Info("Остановка воркера обработки delivery receipts")
				}
				// Сохраняем накопленный batch
				if len(batch) > 0 {
					a.processBatch(batch)
				}
				// Обрабатываем оставшиеся receipts в очереди
				a.drainReceiptQueue()
				return
			}
		}
	}()
}

// processBatch обрабатывает batch delivery receipts
func (a *SMPPAdapter) processBatch(batch []*DeliveryReceipt) {
	if len(batch) == 0 {
		return
	}

	// Отмечаем начало обработки для graceful shutdown
	a.deliveryReceiptWg.Add(1)
	defer a.deliveryReceiptWg.Done()

	// Получаем callbacks
	a.mu.Lock()
	batchCallback := a.batchDeliveryReceiptCallback
	singleCallback := a.deliveryReceiptCallback
	a.mu.Unlock()

	// Приоритет batch callback
	if batchCallback != nil {
		saved := batchCallback(batch)
		if logger.Log != nil {
			logger.Log.Debug("Batch delivery receipts обработан",
				zap.Int("total", len(batch)),
				zap.Int("saved", saved))
		}
		return
	}

	// Fallback на одиночный callback
	if singleCallback != nil {
		for _, receipt := range batch {
			singleCallback(receipt)
		}
		return
	}

	if logger.Log != nil {
		logger.Log.Warn("Batch delivery receipts получен, но callback не установлен",
			zap.Int("count", len(batch)))
	}
}

// drainReceiptQueue обрабатывает все оставшиеся receipts в очереди при остановке (batch mode)
func (a *SMPPAdapter) drainReceiptQueue() {
	batch := make([]*DeliveryReceipt, 0, deliveryReceiptBatchSize)

	// Собираем все оставшиеся receipts
	for {
		select {
		case receipt := <-a.deliveryReceiptQueue:
			if receipt == nil {
				continue
			}
			batch = append(batch, receipt)
			// Если набрали полный batch - сохраняем сразу
			if len(batch) >= deliveryReceiptBatchSize {
				a.processBatch(batch)
				batch = make([]*DeliveryReceipt, 0, deliveryReceiptBatchSize)
			}
		default:
			// Очередь пуста - сохраняем остаток
			if len(batch) > 0 {
				a.processBatch(batch)
				if logger.Log != nil {
					logger.Log.Info("Обработаны оставшиеся delivery receipts при остановке",
						zap.Int("count", len(batch)))
				}
			}
			return
		}
	}
}

// StopDeliveryReceiptWorker останавливает воркер обработки delivery receipts
func (a *SMPPAdapter) StopDeliveryReceiptWorker() {
	a.mu.Lock()
	if !a.receiptWorkerRunning {
		a.mu.Unlock()
		return
	}
	a.receiptWorkerRunning = false
	a.mu.Unlock()

	// Отправляем сигнал остановки
	select {
	case a.receiptWorkerStop <- struct{}{}:
	default:
		// Воркер уже остановлен
	}
}

// GetDeliveryReceiptQueueSize возвращает текущий размер очереди delivery receipts
func (a *SMPPAdapter) GetDeliveryReceiptQueueSize() int {
	return len(a.deliveryReceiptQueue)
}

// handleIncomingPDU обрабатывает входящие PDU (DELIVER_SM для delivery receipts)
func (a *SMPPAdapter) handleIncomingPDU(p pdu.Body) {
	// Проверяем тип PDU - нас интересует только DELIVER_SM
	switch p.Header().ID {
	case pdu.DeliverSMID:
		a.handleDeliverSM(p)
	default:
		// Другие типы PDU игнорируем
		if logger.Log != nil {
			logger.Log.Debug("Получен PDU",
				zap.String("type", p.Header().ID.String()))
		}
	}
}

// handleDeliverSM обрабатывает входящий DELIVER_SM (delivery receipt)
func (a *SMPPAdapter) handleDeliverSM(p pdu.Body) {
	// Отмечаем начало обработки для graceful shutdown
	a.deliveryReceiptWg.Add(1)
	defer a.deliveryReceiptWg.Done()

	// Извлекаем данные из delivery receipt
	fields := p.Fields()

	// Получаем ReceiptedMessageID из TLV параметров
	receiptedMessageID := ""
	if tlvFields := p.TLVFields(); tlvFields != nil {
		if receiptedMsgID := tlvFields[pdutlv.TagReceiptedMessageID]; receiptedMsgID != nil {
			receiptedMessageID = strings.TrimRight(string(receiptedMsgID.Bytes()), "\x00")
		}
	}

	// Получаем MessageState из TLV параметров
	// TLV Tag для MessageState = 0x0427 (1063 в десятичной)
	messageState := 0
	if tlvFields := p.TLVFields(); tlvFields != nil {
		if msgState := tlvFields[pdutlv.Tag(0x0427)]; msgState != nil {
			data := msgState.Bytes()
			if len(data) > 0 {
				messageState = int(data[0])
			}
		}
	}

	// Если ReceiptedMessageID пуст, пробуем извлечь из ShortMessage
	if receiptedMessageID == "" {
		if shortMsg := fields[pdufield.ShortMessage]; shortMsg != nil {
			receiptedMessageID = parseMessageIDFromShortMessage(shortMsg.String())
		}
	}

	// Если MessageState не установлен, пробуем извлечь из ShortMessage
	if messageState == 0 {
		if shortMsg := fields[pdufield.ShortMessage]; shortMsg != nil {
			messageState = parseMessageStateFromShortMessage(shortMsg.String())
		}
	}

	// Определяем статус для сохранения в БД
	var statusID int
	var errorText string

	// SMPP MessageState.DELIVERED = 2 -> статус 4 (доставлено)
	if messageState == SMPPMessageStateDelivered {
		statusID = StatusDelivered // 4 - "Доставлено абоненту"
		errorText = ""
		if logger.Log != nil {
			logger.Log.Info("Delivery receipt: сообщение доставлено абоненту",
				zap.String("messageID", receiptedMessageID),
				zap.Int("smppMessageState", messageState))
		}
	} else {
		statusID = StatusErrorOrUndelivered // 3 - "Не доставлено"
		errorText = fmt.Sprintf("SMS не доставлено до абонента (MessageState=%d)", messageState)
		if logger.Log != nil {
			logger.Log.Warn("Delivery receipt: сообщение не доставлено",
				zap.String("messageID", receiptedMessageID),
				zap.Int("smppMessageState", messageState),
				zap.String("errorText", errorText))
		}
	}

	// Формируем структуру delivery receipt
	receipt := &DeliveryReceipt{
		ReceiptedMessageID: receiptedMessageID,
		MessageState:       messageState,
		SequenceNumber:     p.Header().Seq,
		ErrorText:          errorText,
		ReceivedAt:         time.Now(),
		StatusID:           statusID,
	}

	// Обновляем время последнего ответа
	a.mu.Lock()
	a.lastAnswerTime = time.Now()
	a.mu.Unlock()

	// Добавляем receipt в буферную очередь (неблокирующая операция)
	select {
	case a.deliveryReceiptQueue <- receipt:
		// Успешно добавлено в очередь
		if logger.Log != nil {
			logger.Log.Debug("Delivery receipt добавлен в очередь",
				zap.String("messageID", receiptedMessageID),
				zap.Int("statusID", statusID),
				zap.Int("queueSize", len(a.deliveryReceiptQueue)))
		}
	default:
		// Очередь переполнена - логируем ошибку
		if logger.Log != nil {
			logger.Log.Error("Очередь delivery receipts переполнена, receipt потерян",
				zap.String("messageID", receiptedMessageID),
				zap.Int("statusID", statusID),
				zap.Int("queueSize", deliveryReceiptQueueSize))
		}
	}
}

// parseMessageIDFromShortMessage извлекает MessageID из текста ShortMessage
// Формат: "id:XXXX sub:001 dlvrd:001 submit date:... done date:... stat:DELIVRD err:000 text:..."
func parseMessageIDFromShortMessage(shortMessage string) string {
	// Ищем паттерн "id:" в начале сообщения
	if strings.HasPrefix(strings.ToLower(shortMessage), "id:") {
		parts := strings.Fields(shortMessage)
		if len(parts) > 0 {
			idPart := parts[0]
			if strings.HasPrefix(strings.ToLower(idPart), "id:") {
				return strings.TrimPrefix(strings.TrimPrefix(idPart, "id:"), "ID:")
			}
		}
	}
	return ""
}

// parseMessageStateFromShortMessage извлекает MessageState из текста ShortMessage
// Ищет паттерн "stat:DELIVRD" или "stat:UNDELIV" и т.д.
func parseMessageStateFromShortMessage(shortMessage string) int {
	lowerMsg := strings.ToLower(shortMessage)

	// Ищем паттерн stat: в сообщении
	statIndex := strings.Index(lowerMsg, "stat:")
	if statIndex == -1 {
		return 0
	}

	// Извлекаем значение статуса
	statPart := shortMessage[statIndex+5:]
	parts := strings.Fields(statPart)
	if len(parts) == 0 {
		return 0
	}

	statValue := strings.ToUpper(parts[0])

	// Преобразуем текстовый статус в числовой
	switch statValue {
	case "DELIVRD", "DELIVERED":
		return SMPPMessageStateDelivered // 2
	case "EXPIRED":
		return SMPPMessageStateExpired // 3
	case "DELETED":
		return SMPPMessageStateDeleted // 4
	case "UNDELIV", "UNDELIVERABLE":
		return SMPPMessageStateUndeliverable // 5
	case "ACCEPTD", "ACCEPTED":
		return SMPPMessageStateAccepted // 6
	case "UNKNOWN":
		return SMPPMessageStateUnknown // 7
	case "REJECTD", "REJECTED":
		return SMPPMessageStateRejected // 8
	case "ENROUTE":
		return SMPPMessageStateEnroute // 1
	default:
		return 0
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
		// Логируем только если прошло более 95% интервала и прошло более 5 секунд с последнего логирования
		if timeSinceLastAnswer > rebindInterval*95/100 {
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
		Text:          pdutext.UCS2(text),              // UCS-2 кодировка
		Register:      pdufield.FailureDeliveryReceipt, // OnSuccessOrFailure (0x02)
		SourceAddrTON: sourceTON,
		SourceAddrNPI: sourceNPI,
		DestAddrTON:   destTON,
		DestAddrNPI:   destNPI,
		PriorityFlag:  PriorityHighest, // Highest priority (3)
	}

	// Добавление TLV для AlertOnMsgDelivery (0x1) - пустое значение означает включение уведомления
	req.TLVFields = pdutlv.Fields{
		pdutlv.TagAlertOnMessageDelivery: pdutlv.CString(""),
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
	// Останавливаем воркер обработки delivery receipts
	a.StopDeliveryReceiptWorker()

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.client != nil {
		return a.client.Close()
	}
	return nil
}

// WaitForDeliveryReceipts ожидает завершения всех текущих обработок delivery receipts
// Возвращает true, если все обработки завершились до истечения таймаута
// Возвращает false, если таймаут истек
func (a *SMPPAdapter) WaitForDeliveryReceipts(timeout time.Duration) bool {
	// Защищаем WaitGroup от повторного использования
	a.deliveryReceiptWgMu.Lock()
	defer a.deliveryReceiptWgMu.Unlock()

	// Используем канал для сигнализации завершения и отмены
	done := make(chan struct{})
	cancel := make(chan struct{})
	
	go func() {
		// Вызываем Wait() в горутине
		a.deliveryReceiptWg.Wait()
		
		// Проверяем, не была ли операция отменена
		select {
		case <-cancel:
			// Операция была отменена, просто выходим
			return
		default:
			// Операция завершена успешно
			select {
			case done <- struct{}{}:
			default:
				// Канал уже закрыт или заблокирован
			}
		}
	}()

	select {
	case <-done:
		close(cancel)
		return true
	case <-time.After(timeout):
		// Таймаут - отменяем горутину
		close(cancel)
		// Даем горутине время завершиться
		time.Sleep(10 * time.Millisecond)
		return false
	}
}
