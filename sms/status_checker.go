package sms

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// StatusChecker представляет компонент для проверки статуса доставки SMS
type StatusChecker struct {
	service *Service
	checkWg sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
}

// NewStatusChecker создает новый StatusChecker
func NewStatusChecker(service *Service) *StatusChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &StatusChecker{
		service: service,
		ctx:     ctx,
		cancel:  cancel,
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

	sc.checkWg.Add(1)
	go func() {
		defer sc.checkWg.Done()

		// Получаем контекст для проверки отмены
		sc.mu.RLock()
		ctx := sc.ctx
		sc.mu.RUnlock()

		// Ждем 5 минут перед запросом статуса с возможностью прерывания
		// Если контекст отменится во время ожидания, горутина завершится немедленно
		select {
		case <-ctx.Done():
			// Контекст отменен - завершаем горутину без проверки статуса
			if logger.Log != nil {
				logger.Log.Debug("Проверка статуса отменена (shutdown)",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID))
			}
			return
		case <-time.After(5 * time.Minute):
			// Прошло 5 минут - продолжаем проверку статуса
		}

		// Получаем адаптер для запроса статуса
		sc.service.mu.RLock()
		adapter, ok := sc.service.adapters[smppID]
		sc.service.mu.RUnlock()

		if !ok || adapter == nil {
			if logger.Log != nil {
				logger.Log.Warn("Не удалось получить адаптер для проверки статуса",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID),
					zap.Int("smppID", smppID))
			}
			return
		}

		// Запрашиваем статус доставки
		messageState, err := adapter.QuerySMSStatus(messageID, senderName)
		if err != nil {
			if logger.Log != nil {
				logger.Log.Error("Ошибка запроса статуса доставки SMS",
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
			saveCallback(response)
		} else {
			if logger.Log != nil {
				logger.Log.Warn("Callback для сохранения статуса не установлен",
					zap.Int64("taskID", taskID),
					zap.String("messageID", messageID))
			}
		}
	}()
}

// Stop отменяет контекст, что приводит к немедленному завершению всех горутин проверки статуса
func (sc *StatusChecker) Stop() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.cancel != nil {
		sc.cancel()
	}
}

// Wait ожидает завершения всех горутин проверки статуса
func (sc *StatusChecker) Wait() {
	sc.checkWg.Wait()
}
