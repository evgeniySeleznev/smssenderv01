package sms

import (
	"context"
	"oracle-client/db"
	"oracle-client/logger"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	defaultSmsResponseQueueSize  = 1000
	defaultSmsResponseBatchSize  = 50
	defaultSmsResponseBatchTimeout = 2 * time.Second
)

// SmsResponseQueue представляет очередь для batch-сохранения результатов отправки SMS
type SmsResponseQueue struct {
	queue           chan *SMSResponse
	batchSize       int
	batchTimeout    time.Duration
	dbConn          *db.DBConnection
	stopChan        chan struct{}
	wg              sync.WaitGroup
	started         bool
	startMu         sync.Mutex
	shutdownTimeout time.Duration // Таймаут для graceful shutdown
}

// NewSmsResponseQueue создает новую очередь для batch-сохранения результатов SMS
func NewSmsResponseQueue(dbConn *db.DBConnection) *SmsResponseQueue {
	return &SmsResponseQueue{
		queue:           make(chan *SMSResponse, defaultSmsResponseQueueSize),
		batchSize:       defaultSmsResponseBatchSize,
		batchTimeout:    defaultSmsResponseBatchTimeout,
		dbConn:          dbConn,
		stopChan:        make(chan struct{}),
		shutdownTimeout: 5 * time.Second, // Default value, will be set by main.go
	}
}

// Enqueue добавляет результат SMS в очередь для batch-сохранения
func (q *SmsResponseQueue) Enqueue(response *SMSResponse) {
	if response == nil {
		return
	}
	select {
	case q.queue <- response:
		// Successfully enqueued
	default:
		if logger.Log != nil {
			logger.Log.Error("Очередь результатов SMS переполнена, результат потерян",
				zap.Int64("taskID", response.TaskID),
				zap.String("messageID", response.MessageID))
		}
	}
}

// Start запускает горутину обработки очереди результатов SMS
func (q *SmsResponseQueue) Start(ctx context.Context, shutdownTimeout time.Duration) {
	q.startMu.Lock()
	if q.started {
		q.startMu.Unlock()
		return
	}
	q.started = true
	q.shutdownTimeout = shutdownTimeout // Set the actual shutdown timeout
	q.startMu.Unlock()

	q.wg.Add(1)
	if logger.Log != nil {
		logger.Log.Info("Запущена горутина обработки очереди результатов SMS",
			zap.Int("batchSize", q.batchSize),
			zap.Duration("batchTimeout", q.batchTimeout))
	}

	go q.worker(ctx)
}

// worker обрабатывает очередь результатов SMS, собирая их в batch
func (q *SmsResponseQueue) worker(ctx context.Context) {
	defer q.wg.Done()

	batch := make([]*db.SaveSmsResponseParams, 0, q.batchSize)
	ticker := time.NewTicker(q.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case response := <-q.queue:
			if response == nil {
				continue
			}
			batch = append(batch, &db.SaveSmsResponseParams{
				TaskID:       response.TaskID,
				MessageID:    response.MessageID,
				StatusID:     response.Status,
				ResponseDate: response.SentAt,
				ErrorText:    response.ErrorText,
			})
			if len(batch) >= q.batchSize {
				q.flushBatch(ctx, batch)
				batch = make([]*db.SaveSmsResponseParams, 0, q.batchSize)
				ticker.Reset(q.batchTimeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				q.flushBatch(ctx, batch)
				batch = make([]*db.SaveSmsResponseParams, 0, q.batchSize)
			}

		case <-ctx.Done():
			// Graceful shutdown: flush remaining messages
			if logger.Log != nil {
				logger.Log.Info("Получен сигнал остановки для очереди результатов SMS, начинаем graceful shutdown")
			}
			q.flushBatch(ctx, batch) // Flush any remaining messages
			return

		case <-q.stopChan:
			if logger.Log != nil {
				logger.Log.Info("Остановка горутины обработки очереди результатов SMS")
			}
			q.flushBatch(ctx, batch) // Flush any remaining messages
			return
		}
	}
}

// flushBatch сохраняет batch результатов SMS в БД
func (q *SmsResponseQueue) flushBatch(ctx context.Context, batch []*db.SaveSmsResponseParams) {
	if len(batch) == 0 {
		return
	}
	if logger.Log != nil {
		logger.Log.Info("Сохранение батча результатов SMS в БД", zap.Int("count", len(batch)))
	}

	// Определяем таймаут в зависимости от контекста:
	// - Если контекст отменен (graceful shutdown), используем shutdownTimeout
	// - Иначе используем нормальный таймаут для операций с БД (60 секунд)
	var saveCtx context.Context
	var saveCancel context.CancelFunc
	if ctx.Err() == context.Canceled {
		// Graceful shutdown - используем короткий таймаут
		saveCtx, saveCancel = context.WithTimeout(context.Background(), q.shutdownTimeout)
	} else {
		// Обычная операция - используем нормальный таймаут (60 секунд для batch-операций)
		normalTimeout := 60 * time.Second
		saveCtx, saveCancel = context.WithTimeout(ctx, normalTimeout)
	}
	defer saveCancel()

	// Преобразуем []*db.SaveSmsResponseParams в []db.SaveSmsResponseParams
	params := make([]db.SaveSmsResponseParams, len(batch))
	for i, p := range batch {
		params[i] = *p
	}

	saved, err := q.dbConn.SaveSmsResponseBatch(saveCtx, params)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("Ошибка сохранения батча результатов SMS в БД",
				zap.Int("total", len(batch)),
				zap.Int("saved", saved),
				zap.Error(err))
		}
	} else {
		if logger.Log != nil {
			logger.Log.Debug("Батч результатов SMS успешно сохранен в БД",
				zap.Int("total", len(batch)),
				zap.Int("saved", saved))
		}
	}
}

// Stop останавливает очередь результатов SMS
func (q *SmsResponseQueue) Stop() {
	q.startMu.Lock()
	if !q.started {
		q.startMu.Unlock()
		return
	}
	q.startMu.Unlock()

	select {
	case <-q.stopChan:
		return
	default:
		close(q.stopChan)
	}
	q.wg.Wait()

	q.startMu.Lock()
	q.stopChan = make(chan struct{})
	q.started = false
	q.startMu.Unlock()

	if logger.Log != nil {
		logger.Log.Info("Горутина обработки очереди результатов SMS остановлена")
	}
}

// SetShutdownTimeout устанавливает таймаут для graceful shutdown
func (q *SmsResponseQueue) SetShutdownTimeout(timeout time.Duration) {
	q.startMu.Lock()
	defer q.startMu.Unlock()
	q.shutdownTimeout = timeout
}

// GetQueueSize возвращает текущий размер очереди
func (q *SmsResponseQueue) GetQueueSize() int {
	return len(q.queue)
}

