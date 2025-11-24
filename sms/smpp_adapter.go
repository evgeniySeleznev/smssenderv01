package sms

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutlv"
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
	client         *smpp.Transceiver
	config         *SMPPConfig
	mu             sync.Mutex
	lastAnswerTime time.Time
	statusChan     <-chan smpp.ConnStatus
	isConnected    bool // Флаг подключения
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
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	enquireLink := 10 * time.Second // По умолчанию
	if cfg.EnquireLinkInterval > 0 {
		enquireLink = time.Duration(cfg.EnquireLinkInterval) * time.Second
	}

	client := &smpp.Transceiver{
		Addr:        addr,
		User:        cfg.User,
		Passwd:      cfg.Password,
		EnquireLink: enquireLink,
	}

	adapter.client = client

	return adapter, nil
}

// Bind выполняет подключение к SMPP серверу
func (a *SMPPAdapter) Bind() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	log.Printf("Bind(): подключение к %s:%d, User: %s", a.config.Host, a.config.Port, a.config.User)

	// Если уже подключен, сначала отключаемся
	if a.client != nil && a.IsConnected() {
		log.Printf("  Unbind(): сброс существующего подключения")
		a.unbindInternal()
	}

	// Выполняем Bind и получаем канал статуса
	a.statusChan = a.client.Bind()

	// Запускаем горутину для отслеживания статуса подключения
	statusReceived := make(chan smpp.ConnStatus, 1)
	go func() {
		for status := range a.statusChan {
			a.mu.Lock()
			a.isConnected = (status.Status() == smpp.Connected)
			if !a.isConnected && status.Error() != nil {
				log.Printf("SMPP соединение потеряно: %v", status.Error())
			}
			a.mu.Unlock()
			// Отправляем первый статус в канал для ожидания в Bind
			select {
			case statusReceived <- status:
			default:
			}
		}
		a.mu.Lock()
		a.isConnected = false
		a.mu.Unlock()
	}()

	// Ждем первого статуса подключения
	select {
	case status := <-statusReceived:
		if status.Status() != smpp.Connected {
			err := status.Error()
			if err != nil {
				log.Printf("  Bind(): ошибка подключения: %v", err)
				return fmt.Errorf("не удалось подключиться: %w", err)
			}
			return fmt.Errorf("не удалось подключиться: статус %v", status.Status())
		}
		a.mu.Lock()
		a.isConnected = true
		a.mu.Unlock()
		log.Printf("  Bind(): успешно подключен")
		a.lastAnswerTime = time.Now()
		return nil
	case <-time.After(30 * time.Second):
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

	if a.client == nil || !a.IsConnected() {
		log.Printf("Unbind() не требуется")
		return
	}

	log.Printf("Unbind() отключение")
	a.unbindInternal()
	log.Printf("   Отключились")
}

// IsConnected проверяет, подключен ли клиент
func (a *SMPPAdapter) IsConnected() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.isConnected && a.client != nil
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
		if !a.IsConnected() {
			if err := a.Bind(); err != nil {
				return "", fmt.Errorf("ошибка подключения: %w", err)
			}
		}

		// Отправка SMS с автоматической сегментацией для длинных сообщений
		// Используем SubmitLongMsg для автоматического разбиения длинных сообщений
		// (аналогично UseSmppSegmentation из руководства)
		parts, err := a.client.SubmitLongMsg(req)
		if err != nil {
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
		log.Printf("SendSMS(): ошибка подключения: %v", err)
		// Попытка переподключения и повторной отправки
		if bindErr := a.Bind(); bindErr != nil {
			return "", fmt.Errorf("ошибка переподключения: %w", bindErr)
		}
		smsID, err = trySend()
		if err != nil {
			return "", fmt.Errorf("ошибка отправки после переподключения: %w", err)
		}
	}

	a.lastAnswerTime = time.Now()
	return smsID, nil
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
