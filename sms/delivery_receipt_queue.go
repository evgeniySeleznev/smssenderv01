package sms

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// DeliveryReceiptSaveFunc - функция для batch-сохранения delivery receipts в БД
type DeliveryReceiptSaveFunc func(receipts []*DeliveryReceipt) (successCount int, err error)

// DeliveryReceiptQueue управляет буферизованной очередью delivery receipts
// Сохраняет receipts пачками по batchSize или по истечении flushInterval
type DeliveryReceiptQueue struct {
	queue         chan *DeliveryReceipt
	saveFunc      DeliveryReceiptSaveFunc
	batchSize     int           // Размер пачки для сохранения (по умолчанию 100)
	flushInterval time.Duration // Интервал принудительного сохранения (по умолчанию 2 секунды)
	queueSize     int           // Размер буфера очереди (по умолчанию 10000)

	wg      sync.WaitGroup
	stopCh  chan struct{}
	started bool
	startMu sync.Mutex

	// Статистика
	statsMu         sync.Mutex
	totalReceived   int64
	totalSaved      int64
	totalFailed     int64
	batchesSaved    int64
	lastFlushTime   time.Time
	shutdownTimeout time.Duration
}

// NewDeliveryReceiptQueue создает новую очередь для delivery receipts
func NewDeliveryReceiptQueue(saveFunc DeliveryReceiptSaveFunc) *DeliveryReceiptQueue {
	return &DeliveryReceiptQueue{
		saveFunc:        saveFunc,
		batchSize:       100,             // Сохранять по 100 штук
		flushInterval:   2 * time.Second, // Или каждые 2 секунды
		queueSize:       10000,           // Буфер на 10000 receipts
		stopCh:          make(chan struct{}),
		shutdownTimeout: 5 * time.Second, // Таймаут graceful shutdown
	}
}

// SetBatchSize устанавливает размер пачки для сохранения
func (q *DeliveryReceiptQueue) SetBatchSize(size int) {
	if size > 0 {
		q.batchSize = size
	}
}

// SetFlushInterval устанавливает интервал принудительного сохранения
func (q *DeliveryReceiptQueue) SetFlushInterval(interval time.Duration) {
	if interval > 0 {
		q.flushInterval = interval
	}
}

// SetQueueSize устанавливает размер буфера очереди
func (q *DeliveryReceiptQueue) SetQueueSize(size int) {
	if size > 0 {
		q.queueSize = size
	}
}

// SetShutdownTimeout устанавливает таймаут для graceful shutdown
func (q *DeliveryReceiptQueue) SetShutdownTimeout(timeout time.Duration) {
	q.shutdownTimeout = timeout
}

// Start запускает воркер обработки очереди
func (q *DeliveryReceiptQueue) Start() {
	q.startMu.Lock()
	defer q.startMu.Unlock()

	if q.started {
		return
	}

	q.queue = make(chan *DeliveryReceipt, q.queueSize)
	q.stopCh = make(chan struct{})
	q.started = true
	q.lastFlushTime = time.Now()

	q.wg.Add(1)
	go q.worker()

	if logger.Log != nil {
		logger.Log.Info("Очередь delivery receipts запущена",
			zap.Int("batchSize", q.batchSize),
			zap.Duration("flushInterval", q.flushInterval),
			zap.Int("queueSize", q.queueSize))
	}
}

// Stop останавливает воркер и сохраняет оставшиеся receipts
func (q *DeliveryReceiptQueue) Stop() {
	q.startMu.Lock()
	if !q.started {
		q.startMu.Unlock()
		return
	}
	q.started = false
	q.startMu.Unlock()

	// Закрываем канал остановки
	close(q.stopCh)

	// Ждем завершения воркера
	q.wg.Wait()

	if logger.Log != nil {
		q.statsMu.Lock()
		logger.Log.Info("Очередь delivery receipts остановлена",
			zap.Int64("totalReceived", q.totalReceived),
			zap.Int64("totalSaved", q.totalSaved),
			zap.Int64("totalFailed", q.totalFailed),
			zap.Int64("batchesSaved", q.batchesSaved))
		q.statsMu.Unlock()
	}
}

// Enqueue добавляет delivery receipt в очередь
// Возвращает false, если очередь переполнена
func (q *DeliveryReceiptQueue) Enqueue(receipt *DeliveryReceipt) bool {
	q.startMu.Lock()
	if !q.started {
		q.startMu.Unlock()
		if logger.Log != nil {
			logger.Log.Warn("Попытка добавить receipt в остановленную очередь",
				zap.String("messageID", receipt.ReceiptedMessageID))
		}
		return false
	}
	q.startMu.Unlock()

	select {
	case q.queue <- receipt:
		q.statsMu.Lock()
		q.totalReceived++
		q.statsMu.Unlock()
		return true
	default:
		// Очередь переполнена
		if logger.Log != nil {
			logger.Log.Error("Очередь delivery receipts переполнена, receipt потерян",
				zap.String("messageID", receipt.ReceiptedMessageID),
				zap.Int("queueSize", q.queueSize))
		}
		q.statsMu.Lock()
		q.totalFailed++
		q.statsMu.Unlock()
		return false
	}
}

// GetQueueLength возвращает текущее количество элементов в очереди
func (q *DeliveryReceiptQueue) GetQueueLength() int {
	if q.queue == nil {
		return 0
	}
	return len(q.queue)
}

// GetStats возвращает статистику очереди
func (q *DeliveryReceiptQueue) GetStats() (received, saved, failed, batches int64) {
	q.statsMu.Lock()
	defer q.statsMu.Unlock()
	return q.totalReceived, q.totalSaved, q.totalFailed, q.batchesSaved
}

// worker - основная горутина обработки очереди
func (q *DeliveryReceiptQueue) worker() {
	defer q.wg.Done()

	batch := make([]*DeliveryReceipt, 0, q.batchSize)
	flushTimer := time.NewTimer(q.flushInterval)
	defer flushTimer.Stop()

	for {
		select {
		case <-q.stopCh:
			// Graceful shutdown - сохраняем оставшиеся receipts
			q.drainAndSave(batch)
			return

		case receipt := <-q.queue:
			batch = append(batch, receipt)

			// Если набралась полная пачка - сохраняем
			if len(batch) >= q.batchSize {
				q.saveBatch(batch)
				batch = make([]*DeliveryReceipt, 0, q.batchSize)
				// Сбрасываем таймер после сохранения пачки
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(q.flushInterval)
			}

		case <-flushTimer.C:
			// Таймаут - сохраняем что есть (если есть)
			if len(batch) > 0 {
				q.saveBatch(batch)
				batch = make([]*DeliveryReceipt, 0, q.batchSize)
			}
			flushTimer.Reset(q.flushInterval)
		}
	}
}

// drainAndSave вычитывает оставшиеся receipts из очереди и сохраняет
func (q *DeliveryReceiptQueue) drainAndSave(currentBatch []*DeliveryReceipt) {
	// Создаем контекст с таймаутом для graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), q.shutdownTimeout)
	defer cancel()

	// Сначала сохраняем текущую пачку
	if len(currentBatch) > 0 {
		q.saveBatch(currentBatch)
	}

	// Затем вычитываем и сохраняем оставшиеся из очереди
	batch := make([]*DeliveryReceipt, 0, q.batchSize)

	for {
		select {
		case <-ctx.Done():
			// Таймаут истек - сохраняем что успели собрать и выходим
			if len(batch) > 0 {
				q.saveBatch(batch)
			}
			remaining := len(q.queue)
			if remaining > 0 && logger.Log != nil {
				logger.Log.Warn("Graceful shutdown: не все delivery receipts сохранены",
					zap.Int("lostReceipts", remaining))
			}
			return

		case receipt, ok := <-q.queue:
			if !ok {
				// Канал закрыт
				if len(batch) > 0 {
					q.saveBatch(batch)
				}
				return
			}
			batch = append(batch, receipt)
			if len(batch) >= q.batchSize {
				q.saveBatch(batch)
				batch = make([]*DeliveryReceipt, 0, q.batchSize)
			}

		default:
			// Очередь пуста - сохраняем остаток
			if len(batch) > 0 {
				q.saveBatch(batch)
			}
			if logger.Log != nil {
				logger.Log.Info("Graceful shutdown: все delivery receipts сохранены")
			}
			return
		}
	}
}

// saveBatch сохраняет пачку receipts в БД
func (q *DeliveryReceiptQueue) saveBatch(batch []*DeliveryReceipt) {
	if len(batch) == 0 {
		return
	}

	if q.saveFunc == nil {
		if logger.Log != nil {
			logger.Log.Error("Функция сохранения delivery receipts не установлена",
				zap.Int("lostReceipts", len(batch)))
		}
		q.statsMu.Lock()
		q.totalFailed += int64(len(batch))
		q.statsMu.Unlock()
		return
	}

	startTime := time.Now()
	successCount, err := q.saveFunc(batch)

	q.statsMu.Lock()
	q.batchesSaved++
	q.totalSaved += int64(successCount)
	q.totalFailed += int64(len(batch) - successCount)
	q.lastFlushTime = time.Now()
	q.statsMu.Unlock()

	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("Ошибка batch-сохранения delivery receipts",
				zap.Int("batchSize", len(batch)),
				zap.Int("successCount", successCount),
				zap.Int("failedCount", len(batch)-successCount),
				zap.Duration("duration", time.Since(startTime)),
				zap.Error(err))
		}
	} else {
		if logger.Log != nil {
			logger.Log.Debug("Batch delivery receipts сохранен",
				zap.Int("count", successCount),
				zap.Duration("duration", time.Since(startTime)))
		}
	}
}
