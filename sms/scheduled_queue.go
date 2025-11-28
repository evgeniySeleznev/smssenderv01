package sms

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// ScheduledMessage представляет сообщение, отложенное из-за расписания
type ScheduledMessage struct {
	Message SMSMessage
	AddedAt time.Time
	Reason  string // Причина отложения
}

// ScheduledQueue управляет очередью отложенных сообщений
// Сообщения аккумулируются когда sending_schedule = 1 и текущее время вне окна расписания.
// При наступлении TimeStart на следующий день сообщения отправляются с интервалом 30мс.
// При закрытии программы все сообщения теряются (это нормально).
type ScheduledQueue struct {
	cfg             *Config
	service         *Service // Ссылка на SMS сервис для отправки
	messages        []*ScheduledMessage
	mu              sync.Mutex
	wg              sync.WaitGroup
	stopChan        chan struct{}
	started         bool
	startMu         sync.Mutex
	shutdownTimeout time.Duration // Таймаут для graceful shutdown (по умолчанию 5 секунд)
}

// NewScheduledQueue создает новую очередь отложенных сообщений
func NewScheduledQueue(cfg *Config, service *Service) *ScheduledQueue {
	return &ScheduledQueue{
		cfg:             cfg,
		service:         service,
		messages:        make([]*ScheduledMessage, 0),
		stopChan:        make(chan struct{}),
		shutdownTimeout: 5 * time.Second, // Значение по умолчанию
	}
}

// SetShutdownTimeout устанавливает таймаут для graceful shutdown
func (q *ScheduledQueue) SetShutdownTimeout(timeout time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdownTimeout = timeout
}

// IsWithinSchedule проверяет, находится ли текущее время в пределах окна расписания
func (q *ScheduledQueue) IsWithinSchedule() bool {
	now := time.Now()

	// Извлекаем часы и минуты из TimeStart и TimeEnd
	timeStartHour := q.cfg.Schedule.TimeStart.Hour()
	timeStartMin := q.cfg.Schedule.TimeStart.Minute()
	timeEndHour := q.cfg.Schedule.TimeEnd.Hour()
	timeEndMin := q.cfg.Schedule.TimeEnd.Minute()

	// Формируем время начала и конца рабочего дня для сегодня
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	timeStart := today.Add(time.Duration(timeStartHour)*time.Hour + time.Duration(timeStartMin)*time.Minute)
	timeEnd := today.Add(time.Duration(timeEndHour)*time.Hour + time.Duration(timeEndMin)*time.Minute)

	// Проверяем, попадает ли текущее время в интервал [TimeStart, TimeEnd]
	return !now.Before(timeStart) && !now.After(timeEnd)
}

// Enqueue добавляет сообщение в очередь отложенных
func (q *ScheduledQueue) Enqueue(msg SMSMessage, reason string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	scheduledMsg := &ScheduledMessage{
		Message: msg,
		AddedAt: time.Now(),
		Reason:  reason,
	}

	q.messages = append(q.messages, scheduledMsg)

	if logger.Log != nil {
		logger.Log.Info("Сообщение добавлено в очередь отложенных",
			zap.Int64("taskID", msg.TaskID),
			zap.String("reason", reason),
			zap.Int("queueSize", len(q.messages)))
	}
}

// GetQueueSize возвращает текущий размер очереди
func (q *ScheduledQueue) GetQueueSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// calculateNextScheduleStart вычисляет время следующего TimeStart
// Если сейчас до TimeStart сегодня - возвращает TimeStart сегодня
// Если сейчас после TimeStart сегодня - возвращает TimeStart завтра
func (q *ScheduledQueue) calculateNextScheduleStart() time.Time {
	now := time.Now()

	// Извлекаем часы и минуты из TimeStart
	timeStartHour := q.cfg.Schedule.TimeStart.Hour()
	timeStartMin := q.cfg.Schedule.TimeStart.Minute()

	// Формируем TimeStart на сегодня
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	timeStartToday := today.Add(time.Duration(timeStartHour)*time.Hour + time.Duration(timeStartMin)*time.Minute)

	// Если текущее время до TimeStart сегодня - возвращаем TimeStart сегодня
	if now.Before(timeStartToday) {
		return timeStartToday
	}

	// Иначе возвращаем TimeStart завтра
	tomorrow := today.Add(24 * time.Hour)
	return tomorrow.Add(time.Duration(timeStartHour)*time.Hour + time.Duration(timeStartMin)*time.Minute)
}

// Start запускает горутину обработки отложенных сообщений
func (q *ScheduledQueue) Start(ctx context.Context) {
	q.startMu.Lock()
	if q.started {
		q.startMu.Unlock()
		return
	}
	q.started = true
	q.startMu.Unlock()

	q.wg.Add(1)

	if logger.Log != nil {
		logger.Log.Info("Запущена горутина отложенной отправки SMS",
			zap.String("timeStart", q.cfg.Schedule.TimeStart.Format("15:04")),
			zap.String("timeEnd", q.cfg.Schedule.TimeEnd.Format("15:04")))
	}

	go q.worker(ctx)
}

// worker - основная горутина обработки отложенных сообщений
func (q *ScheduledQueue) worker(ctx context.Context) {
	defer q.wg.Done()

	for {
		// Если сейчас в окне расписания — обрабатываем очередь
		if q.IsWithinSchedule() {
			q.processQueue(ctx)
		}

		// Вычисляем время до следующего TimeStart
		nextStart := q.calculateNextScheduleStart()
		waitDuration := time.Until(nextStart)

		if logger.Log != nil {
			logger.Log.Debug("Очередь отложенных SMS: ожидание до следующего TimeStart",
				zap.Time("nextStart", nextStart),
				zap.Duration("waitDuration", waitDuration),
				zap.Int("queueSize", q.GetQueueSize()))
		}

		// Создаем таймер для ожидания до следующего TimeStart
		timer := time.NewTimer(waitDuration)

		select {
		case <-ctx.Done():
			timer.Stop()
			// Graceful shutdown - даем shutdownTimeout секунд на завершение
			if logger.Log != nil {
				logger.Log.Info("Получен сигнал остановки для очереди отложенных SMS, начинаем graceful shutdown")
			}
			q.gracefulShutdown()
			return

		case <-q.stopChan:
			timer.Stop()
			if logger.Log != nil {
				logger.Log.Info("Остановка горутины отложенной отправки SMS")
			}
			return

		case <-timer.C:
			// Наступило время TimeStart — обрабатываем очередь
			if logger.Log != nil {
				logger.Log.Info("Наступило время отправки отложенных SMS",
					zap.Int("queueSize", q.GetQueueSize()))
			}
			q.processQueue(ctx)
		}
	}
}

// processQueue обрабатывает все накопленные сообщения
func (q *ScheduledQueue) processQueue(ctx context.Context) {
	// Атомарно забираем все сообщения из очереди
	q.mu.Lock()
	if len(q.messages) == 0 {
		q.mu.Unlock()
		return
	}

	// Копируем и очищаем очередь
	messages := q.messages
	q.messages = make([]*ScheduledMessage, 0)
	q.mu.Unlock()

	if logger.Log != nil {
		logger.Log.Info("Начинается отправка отложенных SMS",
			zap.Int("count", len(messages)))
	}

	// Отправляем сообщения с интервалом 30мс
	sentCount := 0
	for i, scheduledMsg := range messages {
		// Проверяем контекст перед каждой отправкой
		select {
		case <-ctx.Done():
			// При отмене контекста просто выходим - сообщения будут потеряны при закрытии приложения
			if logger.Log != nil {
				logger.Log.Warn("Отправка отложенных SMS прервана из-за graceful shutdown",
					zap.Int("sent", sentCount),
					zap.Int("remaining", len(messages)-sentCount))
			}
			return
		default:
		}

		// Отправляем SMS
		q.sendScheduledMessage(ctx, scheduledMsg)
		sentCount++

		// Задержка 30мс между отправками (кроме последнего сообщения)
		if i < len(messages)-1 {
			select {
			case <-ctx.Done():
				// При отмене контекста просто выходим
				if logger.Log != nil {
					logger.Log.Warn("Отправка отложенных SMS прервана во время задержки",
						zap.Int("sent", sentCount),
						zap.Int("remaining", len(messages)-sentCount))
				}
				return
			case <-time.After(30 * time.Millisecond):
			}
		}
	}

	if logger.Log != nil {
		logger.Log.Info("Все отложенные SMS успешно обработаны",
			zap.Int("count", len(messages)))
	}
}

// sendScheduledMessage отправляет одно отложенное сообщение
func (q *ScheduledQueue) sendScheduledMessage(ctx context.Context, scheduledMsg *ScheduledMessage) {
	msg := scheduledMsg.Message

	// Убираем флаг SendingSchedule, чтобы избежать повторной проверки расписания
	// (мы уже проверили, что сейчас время отправки)
	msg.SendingSchedule = false

	if logger.Log != nil {
		logger.Log.Debug("Отправка отложенного SMS",
			zap.Int64("taskID", msg.TaskID),
			zap.String("phone", msg.PhoneNumber),
			zap.Time("addedAt", scheduledMsg.AddedAt),
			zap.Duration("waitTime", time.Since(scheduledMsg.AddedAt)))
	}

	// Используем ProcessSMS для отправки с полной обработкой
	response, err := q.service.ProcessSMS(ctx, msg)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("Ошибка отправки отложенного SMS",
				zap.Int64("taskID", msg.TaskID),
				zap.Error(err))
		}
		return
	}

	if response != nil {
		if logger.Log != nil {
			logger.Log.Info("Отложенное SMS обработано",
				zap.Int64("taskID", response.TaskID),
				zap.Int("status", response.Status),
				zap.String("messageID", response.MessageID),
				zap.String("errorText", response.ErrorText))
		}

		// Вызываем callback для сохранения результата в БД
		saveCallback := q.service.GetSaveCallback()
		if saveCallback != nil {
			saveCallback(response)
		}
	}
}

// gracefulShutdown выполняет корректное завершение с таймаутом
func (q *ScheduledQueue) gracefulShutdown() {
	q.mu.Lock()
	queueSize := len(q.messages)
	timeout := q.shutdownTimeout
	q.mu.Unlock()

	if queueSize == 0 {
		if logger.Log != nil {
			logger.Log.Info("Очередь отложенных SMS пуста, завершение без потерь")
		}
		return
	}

	if logger.Log != nil {
		logger.Log.Warn("Graceful shutdown: сообщения в очереди отложенных будут потеряны",
			zap.Int("queueSize", queueSize),
			zap.Duration("shutdownTimeout", timeout))
	}

	// Создаем контекст с таймаутом для последней попытки отправки
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Пытаемся отправить сколько успеем за 5 секунд
	// Только если мы находимся в окне расписания
	if q.IsWithinSchedule() {
		q.processQueue(shutdownCtx)
	}

	// Логируем оставшиеся сообщения
	q.mu.Lock()
	remaining := len(q.messages)
	q.mu.Unlock()

	if remaining > 0 {
		if logger.Log != nil {
			logger.Log.Warn("Graceful shutdown завершен: сообщения потеряны",
				zap.Int("lostMessages", remaining))
		}
	} else {
		if logger.Log != nil {
			logger.Log.Info("Graceful shutdown: все отложенные SMS успешно отправлены")
		}
	}
}

// Stop останавливает горутину обработки отложенных сообщений
func (q *ScheduledQueue) Stop() {
	q.startMu.Lock()
	if !q.started {
		q.startMu.Unlock()
		return
	}
	q.startMu.Unlock()

	// Проверяем, не закрыт ли уже канал
	select {
	case <-q.stopChan:
		// Уже остановлен
		return
	default:
		close(q.stopChan)
	}

	// Ждем завершения горутины
	q.wg.Wait()

	// Создаем новый канал для возможного повторного запуска
	q.startMu.Lock()
	q.stopChan = make(chan struct{})
	q.started = false
	q.startMu.Unlock()

	if logger.Log != nil {
		logger.Log.Info("Горутина отложенной отправки SMS остановлена")
	}
}
