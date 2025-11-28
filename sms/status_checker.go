package sms

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// StatusChecker представляет компонент для проверки статуса доставки SMS
type StatusChecker struct {
	service *Service
}

// NewStatusChecker создает новый StatusChecker
func NewStatusChecker(service *Service) *StatusChecker {
	return &StatusChecker{
		service: service,
	}
}

// StartStatusCheck запускает горутину для проверки статуса доставки SMS через 5 минут
// taskID - ID задачи из БД
// messageID - ID сообщения от SMPP провайдера
// senderName - имя отправителя (source address)
// smppID - ID SMPP провайдера
func (sc *StatusChecker) StartStatusCheck(taskID int64, messageID, senderName string, smppID int) {
	// Запускаем горутину только если это не режим Silent
	if sc.service.cfg.Mode.Silent {
		if logger.Log != nil {
			logger.Log.Debug("Проверка статуса пропущена (режим Silent)",
				zap.Int64("taskID", taskID),
				zap.String("messageID", messageID))
		}
		return
	}

	go func() {
		// Ждем 5 минут перед запросом статуса
		// Используем простой time.Sleep - горутина может проснуться после закрытия соединений
		time.Sleep(5 * time.Minute)

		// Получаем адаптер для запроса статуса
		sc.service.mu.RLock()
		adapter, ok := sc.service.adapters[smppID]
		sc.service.mu.RUnlock()

		if !ok || adapter == nil {
			if logger.Log != nil {
				logger.Log.Warn("Не удалось получить адаптер для проверки статуса (адаптер не найден или удален)",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID),
					zap.Int("smppID", smppID))
			}
			return
		}

		// Проверяем, подключен ли адаптер (может быть закрыт при shutdown)
		if !adapter.IsConnected() {
			if logger.Log != nil {
				logger.Log.Warn("SMPP адаптер не подключен при проверке статуса (возможно, соединение закрыто при shutdown)",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID),
					zap.Int("smppID", smppID))
			}
			return
		}

		// Запрашиваем статус доставки
		messageState, err := adapter.QuerySMSStatus(messageID, senderName)
		if err != nil {
			// Обрабатываем ошибки соединения (может быть закрыто при shutdown)
			if logger.Log != nil {
				logger.Log.Error("Ошибка запроса статуса доставки SMS (возможно, соединение закрыто при shutdown)",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID),
					zap.Error(err))
			}
			return
		}

		// Определяем статус для сохранения в БД
		// messageState: 2 = DELIVERED, 3 = EXPIRED, 5 = UNDELIVERABLE
		// statusID: 3 = не доставлено, 4 = доставлено
		var statusID int
		var errorText string

		if messageState == 2 { // DELIVERED
			statusID = 4 // Доставлено абоненту
			errorText = ""
			if logger.Log != nil {
				logger.Log.Info("SMS доставлено абоненту",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID),
					zap.Int("messageState", messageState))
			}
		} else {
			statusID = 3 // Не доставлено
			errorText = fmt.Sprintf("SMS не доставлено (MessageState=%d)", messageState)
			if logger.Log != nil {
				logger.Log.Warn("SMS не доставлено",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID),
					zap.Int("messageState", messageState))
			}
		}

		// Обновляем статус в БД через callback
		sc.service.mu.RLock()
		saveCallback := sc.service.saveCallback
		sc.service.mu.RUnlock()

		if saveCallback != nil {
			response := &SMSResponse{
				TaskID:    taskID,
				MessageID: messageID,
				Status:    statusID,
				ErrorText: errorText,
				SentAt:    time.Now(),
			}
			// Вызываем callback - он может вернуть ошибку, если БД соединение закрыто
			// Ошибка будет обработана внутри callback (в main.go), но мы логируем здесь для ясности
			saveCallback(response)
		} else {
			if logger.Log != nil {
				logger.Log.Warn("Callback для сохранения статуса не установлен (возможно, приложение завершается)",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID))
			}
		}
	}()
}

// Stop останавливает проверку статусов (для совместимости, но не прерывает горутины с time.Sleep)
// Горутины продолжат работу и обработают ошибки соединения при попытке использования закрытых ресурсов
func (sc *StatusChecker) Stop() {
	if logger.Log != nil {
		logger.Log.Info("Остановка проверки статусов (горутины могут продолжить работу после закрытия соединений)")
	}
}
