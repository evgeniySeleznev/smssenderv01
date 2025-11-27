package main

import (
	"context"
	"encoding/json"
	"oracle-client/db"
	"oracle-client/logger"
	"oracle-client/sms"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func main() {
	// Создаем подключение к БД для загрузки конфигурации
	dbConn, err := db.NewDBConnection()
	if err != nil {
		// Используем стандартный вывод для критических ошибок до инициализации логгера
		os.Stderr.WriteString("Ошибка создания подключения: " + err.Error() + "\n")
		os.Exit(1)
	}
	defer dbConn.CloseConnection()

	// Инициализируем логгер с конфигурацией из БД
	if err := logger.InitLogger(dbConn.GetConfig()); err != nil {
		os.Stderr.WriteString("Ошибка инициализации логгера: " + err.Error() + "\n")
		os.Exit(1)
	}
	defer logger.Log.Sync()

	// Создаем контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Таймаут для завершения операций при graceful shutdown
	// Дает время на завершение отправки SMS, обработки delivery receipts и записи в БД
	// Единая константа для всего приложения
	const shutdownTimeout = 5 * time.Second

	// Настраиваем обработку сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	// Обрабатываем основные сигналы, которые работают на всех платформах
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	// Также обрабатываем SIGHUP на Unix системах (на Windows не поддерживается)
	if runtime.GOOS != "windows" {
		signal.Notify(sigChan, syscall.SIGHUP)
	}

	// Горутина для обработки сигналов
	shutdownRequested := make(chan struct{})
	go func() {
		sig := <-sigChan
		logger.Log.Info("Получен сигнал, инициируется graceful shutdown",
			zap.String("signal", sig.String()),
			zap.Duration("shutdownTimeout", shutdownTimeout))
		close(shutdownRequested)
	}()

	// Открываем соединение
	if err := dbConn.OpenConnection(); err != nil {
		logger.Log.Fatal("Ошибка открытия соединения", zap.Error(err))
	}

	logger.Log.Info("Успешно подключено к Oracle базе данных")

	// Запускаем механизм периодического переподключения к БД (каждые 300 секунд)
	dbConn.StartPeriodicReconnect()

	// Загружаем конфигурацию SMS сервиса
	smsConfig, err := sms.LoadConfig(dbConn.GetConfig())
	if err != nil {
		logger.Log.Fatal("Ошибка загрузки конфигурации SMS", zap.Error(err))
	}
	logger.Log.Info("Конфигурация SMS загружена", zap.Bool("silent", smsConfig.Mode.Silent))

	// Создаем SMS сервис
	smsService := sms.NewService(smsConfig)

	// Устанавливаем функцию получения тестового номера для режима Debug
	smsService.SetTestPhoneGetter(dbConn.GetTestPhone)

	// Устанавливаем единый таймаут для graceful shutdown
	smsService.SetShutdownTimeout(shutdownTimeout)

	// Создаем очередь для batch-сохранения результатов SMS
	smsResponseQueue := sms.NewSmsResponseQueue(dbConn)
	smsResponseQueue.Start(ctx, shutdownTimeout)

	// Инициализируем SMPP адаптеры заранее (подключаемся ко всем провайдерам)
	if err := smsService.InitializeAdapters(); err != nil {
		logger.Log.Warn("Ошибка инициализации адаптеров", zap.Error(err))
		logger.Log.Info("Адаптеры будут инициализированы при первой отправке SMS")
	}

	// Устанавливаем callback для сохранения результатов отложенных SMS в БД
	// Используем batch-очередь для лучшей производительности
	smsService.SetSaveCallback(func(response *sms.SMSResponse) {
		if response == nil {
			return
		}
		smsResponseQueue.Enqueue(response)
	})

	// Устанавливаем batch callback для обработки delivery receipts (статус 4 - доставлено)
	// Batch формируется по 50 receipts или каждые 2 секунды (что раньше)
	// TaskID = -1 означает NULL в БД, связь устанавливается через MessageID
	smsService.SetBatchDeliveryReceiptCallback(func(receipts []*sms.DeliveryReceipt) int {
		if len(receipts) == 0 {
			return 0
		}

		// Преобразуем receipts в параметры для batch-сохранения
		params := make([]db.SaveSmsResponseParams, len(receipts))
		for i, receipt := range receipts {
			params[i] = db.SaveSmsResponseParams{
				TaskID:       -1, // NULL в БД - связь через MessageID
				MessageID:    receipt.ReceiptedMessageID,
				StatusID:     receipt.StatusID, // 4 = доставлено, 3 = не доставлено
				ResponseDate: receipt.ReceivedAt,
				ErrorText:    receipt.ErrorText,
			}
		}

		// Batch-сохранение в БД
		saved, err := dbConn.SaveSmsResponseBatch(context.Background(), params)
		if err != nil {
			logger.Log.Error("Ошибка batch-сохранения delivery receipts в БД",
				zap.Int("total", len(receipts)),
				zap.Int("saved", saved),
				zap.Error(err))
		} else {
			// Считаем delivered/undelivered для логирования
			delivered := 0
			for _, r := range receipts {
				if r.StatusID == sms.StatusDelivered {
					delivered++
				}
			}
			logger.Log.Info("Batch delivery receipts сохранен",
				zap.Int("total", len(receipts)),
				zap.Int("delivered", delivered),
				zap.Int("undelivered", len(receipts)-delivered))
		}

		return saved
	})

	// Запускаем очередь отложенных сообщений
	smsService.StartScheduledQueue(ctx)

	// Создаем QueueReader
	queueReader, err := db.NewQueueReader(dbConn)
	if err != nil {
		logger.Log.Fatal("Ошибка создания QueueReader", zap.Error(err))
	}

	logger.Log.Info("Настройки очереди",
		zap.String("queue", queueReader.GetQueueName()),
		zap.String("consumer", queueReader.GetConsumerName()))
	logger.Log.Info("Начало работы с очередью Oracle AQ...")

	// Устанавливаем таймаут ожидания сообщений (аналогично Python: queue.deqoptions.wait = 10)
	queueReader.SetWaitTimeout(10)

	// Проверяем, нужно ли обрабатывать exception queue
	cfg := dbConn.GetConfig()
	var processExceptionQueue bool
	if cfg != nil {
		queueSec := cfg.Section("queue")
		processExceptionQueueStr := strings.ToLower(strings.TrimSpace(queueSec.Key("process_exception_queue").String()))
		processExceptionQueue, _ = strconv.ParseBool(processExceptionQueueStr)
	}

	var exceptionQueueReader *db.ExceptionQueueReader
	if processExceptionQueue {
		exceptionQueueReader, err = db.NewExceptionQueueReader(dbConn)
		if err != nil {
			logger.Log.Warn("Не удалось создать ExceptionQueueReader", zap.Error(err))
			logger.Log.Info("Продолжаем работу без обработки exception queue")
			processExceptionQueue = false
		} else {
			logger.Log.Info("Обработка exception queue включена", zap.String("queue", exceptionQueueReader.GetQueueName()))
			exceptionQueueReader.SetWaitTimeout(25)
		}
	}

	// Переменная для хранения shutdown контекста (устанавливается при graceful shutdown)
	var shutdownCtxForHandlers context.Context
	var shutdownCtxMu sync.RWMutex

	// Функция для асинхронной обработки батча сообщений
	// ВАЖНО: Сообщения уже вычитаны из очереди Oracle, поэтому мы ОБЯЗАНЫ обработать их до конца,
	// иначе они будут потеряны. При graceful shutdown мы прекращаем только чтение новых сообщений,
	// но продолжаем обрабатывать уже вычитанные.
	// Используем замыкание для доступа к smsResponseQueue
	processMessagesBatch := func(ctx context.Context, messages []*db.QueueMessage, batchNum int) {
		// При graceful shutdown используем shutdownCtx вместо отмененного ctx
		shutdownCtxMu.RLock()
		activeCtx := shutdownCtxForHandlers
		shutdownCtxMu.RUnlock()

		// Если shutdownCtx установлен и основной контекст отменен, используем shutdownCtx
		originalCtxCanceled := ctx.Err() == context.Canceled
		if activeCtx != nil && originalCtxCanceled {
			if logger.Log != nil {
				logger.Log.Info("Используется shutdownCtx для завершения обработки батча",
					zap.Int("batchNum", batchNum),
					zap.Int("count", len(messages)),
					zap.Bool("originalCtxCanceled", originalCtxCanceled))
			}
			ctx = activeCtx
		}
		logger.Log.Debug("Обработка батча",
			zap.Int("batchNum", batchNum),
			zap.Int("count", len(messages)),
			zap.Bool("usingShutdownCtx", activeCtx != nil && originalCtxCanceled))
		for i, msg := range messages {
			// НЕ прерываем обработку уже вычитанных сообщений - они должны быть обработаны до конца
			// Проверка контекста здесь только для логирования, но не для прерывания

			logger.Log.Debug("Обработка сообщения",
				zap.Int("messageNum", i+1),
				zap.Int("batchNum", batchNum),
				zap.String("messageID", msg.MessageID),
				zap.Time("dequeueTime", msg.DequeueTime))

			// Парсим XML сообщение
			parsed, err := queueReader.ParseXMLMessage(msg)
			if err != nil {
				logger.Log.Error("Ошибка парсинга XML", zap.Error(err), zap.String("messageID", msg.MessageID))
				continue
			}

			// Выводим распарсенные данные в JSON формате для читаемости
			jsonData, _ := json.MarshalIndent(parsed, "", "  ")
			logger.Log.Debug("Распарсенные данные", zap.String("data", string(jsonData)))

			// Преобразуем распарсенные данные в SMSMessage
			smsMsg, err := sms.ParseSMSMessage(parsed)
			if err != nil {
				logger.Log.Error("Ошибка преобразования в SMSMessage", zap.Error(err))
				continue
			}

			// Обрабатываем и отправляем SMS
			// Передаем контекст для возможности отмены при graceful shutdown
			response, err := smsService.ProcessSMS(ctx, *smsMsg)

			// Проверяем, было ли сообщение отложено (schedule=1 вне окна расписания)
			// В этом случае ProcessSMS возвращает nil, nil - сообщение будет отправлено позже
			if response == nil && err == nil {
				logger.Log.Debug("SMS добавлено в очередь отложенных, пропускаем",
					zap.Int64("taskID", smsMsg.TaskID))
				continue
			}

			if err != nil {
				// Проверяем, была ли операция отменена из-за graceful shutdown
				if ctx.Err() == context.Canceled {
					logger.Log.Warn("Обработка SMS отменена из-за graceful shutdown",
						zap.Int64("taskID", smsMsg.TaskID))
					// ВАЖНО: Сообщение уже вычитано из очереди (REMOVE), поэтому ОБЯЗАНЫ сохранить статус в БД
					// даже если SMS не был отправлен, чтобы зафиксировать факт обработки
					// Это предотвращает потерю данных при graceful shutdown
					// Используем batch-очередь для сохранения
					if response != nil {
						smsResponseQueue.Enqueue(response)
						logger.Log.Debug("Результат отмененного SMS добавлен в очередь для сохранения",
							zap.Int64("taskID", response.TaskID))
					}
					continue
				}
				logger.Log.Error("Ошибка обработки SMS", zap.Error(err))
				// Даже при ошибке сохраняем результат в БД (статус ошибки)
				// Используем batch-очередь для сохранения
				if response != nil {
					smsResponseQueue.Enqueue(response)
				}
				continue
			}

			// Логируем результат
			logger.Log.Info("Результат отправки SMS",
				zap.Int64("taskID", response.TaskID),
				zap.Int("status", response.Status),
				zap.String("messageID", response.MessageID),
				zap.String("errorText", response.ErrorText))

			// Сохраняем результат в БД через batch-очередь
			// Batch-сохранение происходит асинхронно по 50 результатов или каждые 2 секунды
			smsResponseQueue.Enqueue(response)

			// Задержка между обработкой сообщений (аналогично C#: Thread.Sleep(20))
			// НЕ прерываем задержку - сообщения уже вычитаны из очереди и должны быть обработаны
			time.Sleep(20 * time.Millisecond)
		}
		logger.Log.Debug("Батч обработан", zap.Int("batchNum", batchNum))
	}

	// WaitGroup для отслеживания всех обработчиков сообщений
	var allHandlersWg sync.WaitGroup

	// Запускаем основной цикл обработки очереди в отдельной горутине
	// чтобы можно было отслеживать shutdown request
	go func() {
		runQueueProcessingLoop(ctx, cancel, queueReader, exceptionQueueReader, smsService, dbConn, processExceptionQueue, &allHandlersWg, processMessagesBatch, smsResponseQueue)
	}()

	// Ждем сигнала shutdown
	<-shutdownRequested

	logger.Log.Info("Начало graceful shutdown с таймаутом",
		zap.Duration("timeout", shutdownTimeout))

	// Создаем контекст с таймаутом для завершения операций
	// Это дает дополнительное время на завершение отправки SMS и записи в БД
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Устанавливаем shutdownCtx для обработчиков сообщений
	// Это позволит им использовать shutdownCtx вместо отмененного ctx
	shutdownCtxMu.Lock()
	shutdownCtxForHandlers = shutdownCtx
	shutdownCtxMu.Unlock()

	if logger.Log != nil {
		logger.Log.Info("shutdownCtx установлен для обработчиков сообщений, отменяем основной контекст")
	}

	// Отменяем основной контекст, чтобы прекратить чтение новых сообщений
	// НО уже запущенные операции продолжат работу до истечения shutdownCtx таймаута
	cancel()

	// Создаем канал для отслеживания завершения операций
	operationsDone := make(chan struct{})
	go func() {
		// Ждем завершения всех обработчиков сообщений
		allHandlersWg.Wait()
		close(operationsDone)
	}()

	// Ждем либо завершения операций, либо истечения таймаута
	select {
	case <-operationsDone:
		logger.Log.Info("Все операции завершены до истечения таймаута")
	case <-shutdownCtx.Done():
		logger.Log.Warn("Таймаут graceful shutdown истек, принудительное завершение",
			zap.Duration("timeout", shutdownTimeout),
			zap.Int32("activeOperations", dbConn.GetActiveOperationsCount()))
	}

	// Graceful shutdown: завершаем все компоненты
	performGracefulShutdown(shutdownCtx, smsService, dbConn, &allHandlersWg, shutdownTimeout, smsResponseQueue)
}

// runQueueProcessingLoop выполняет основной цикл чтения и обработки сообщений из очереди
func runQueueProcessingLoop(
	ctx context.Context,
	cancel context.CancelFunc,
	queueReader *db.QueueReader,
	exceptionQueueReader *db.ExceptionQueueReader,
	smsService *sms.Service,
	dbConn *db.DBConnection,
	processExceptionQueue bool,
	allHandlersWg *sync.WaitGroup,
	processMessagesBatch func(context.Context, []*db.QueueMessage, int),
	smsResponseQueue *sms.SmsResponseQueue,
) {
	// Бесконечный цикл чтения из очереди (аналогично Python: while True)
	for {
		// Проверяем контекст перед началом итерации
		if ctx.Err() != nil {
			logger.Log.Info("Получен сигнал остановки, прекращаем чтение из очереди...")
			return
		}

		// Создаем новый канал для сигнала на каждой итерации
		iterationSignalChan := make(chan struct{})

		// Запускаем горутину с таймером на 2 минуты
		allHandlersWg.Add(1)
		go func() {
			defer allHandlersWg.Done()
			timer := time.NewTimer(2 * time.Minute)
			defer timer.Stop()

			select {
			case <-timer.C:
				// Таймер сработал - не получили ответ за 2 минуты
				logger.Log.Warn("Таймаут: не получен ответ из DequeueMany в течение 2 минут, завершаем работу...")
				cancel()
			case <-iterationSignalChan:
				// Получили сигнал - ответ получен, прекращаем работу горутины
				return
			case <-ctx.Done():
				// Получен сигнал остановки
				return
			}
		}()

		// Перед чтением проверяем наличие доступных SMPP соединений, чтобы не вычитывать сообщения впустую
		if !smsService.EnsureSMPPConnectivity() {
			logger.Log.Warn("Нет доступных SMPP соединений, чтение очереди пропущено")
			if !sleepWithContext(ctx, 5*time.Second) {
				return
			}
			continue
		}

		// Получаем сообщения из основной очереди (аналогично Python: messages = queue.deqmany(settings.query_number))
		// Используем DequeueMany с количеством сообщений (аналогично settings.query_number = 100)
		// Передаем контекст для возможности отмены операций при graceful shutdown
		messages, err := queueReader.DequeueMany(ctx, 100)

		// Отправляем сигнал в горутину о получении ответа (независимо от результата)
		close(iterationSignalChan)

		// При graceful shutdown: обрабатываем уже вычитанные сообщения перед выходом
		// DequeueMany возвращает сообщения даже при отмене контекста
		gracefulShutdownInProgress := ctx.Err() == context.Canceled

		if err != nil && !gracefulShutdownInProgress {
			// Обычная ошибка (не graceful shutdown) - переподключение
			logger.Log.Error("Ошибка при выборке сообщений", zap.Error(err))
			logger.Log.Info("Ошибка соединения, переподключение...")
			if !sleepWithContext(ctx, 5*time.Second) {
				return
			}
			if err := dbConn.Reconnect(); err != nil {
				logger.Log.Error("Ошибка переподключения", zap.Error(err))
				if !sleepWithContext(ctx, 5*time.Second) {
					return
				}
			}
			continue
		}

		// При graceful shutdown: если есть вычитанные сообщения, обрабатываем их
		if gracefulShutdownInProgress {
			if len(messages) > 0 {
				logger.Log.Info("Graceful shutdown: обработка вычитанных сообщений перед завершением",
					zap.Int("count", len(messages)))
			} else {
				logger.Log.Info("Выборка сообщений отменена из-за graceful shutdown")
				return
			}
		}

		if len(messages) == 0 {
			// Очередь пуста - логируем и продолжаем цикл
			// Аналогично Python: logging.info(f"Очередь {self.connType} пуста в течение {settings.query_wait_time} секунд, перезапускаю слушатель")
			logger.Log.Debug("Очередь пуста, ожидание следующей попытки...")
		} else {
			// Обрабатываем полученные сообщения асинхронно батчами по 100
			logger.Log.Info("Получено сообщений", zap.Int("count", len(messages)))

			// Разбиваем на батчи по 10 сообщений для асинхронной обработки
			batchSize := 10
			var wg sync.WaitGroup

			for i := 0; i < len(messages); i += batchSize {
				// НЕ проверяем контекст здесь - сообщения уже вычитаны из очереди
				// и должны быть обработаны до конца, даже при graceful shutdown
				// Проверка контекста происходит внутри processMessagesBatch

				end := i + batchSize
				if end > len(messages) {
					end = len(messages)
				}
				batch := messages[i:end]
				batchNum := (i / batchSize) + 1

				wg.Add(1)
				allHandlersWg.Add(1)
				go func(batch []*db.QueueMessage, num int) {
					defer wg.Done()
					defer allHandlersWg.Done()
					processMessagesBatch(ctx, batch, num)
				}(batch, batchNum)
			}

			// Ждем завершения обработки всех батчей
			wg.Wait()
			logger.Log.Debug("Все батчи обработаны")

			// При graceful shutdown: выходим после обработки вычитанных сообщений
			if gracefulShutdownInProgress {
				logger.Log.Info("Graceful shutdown: все вычитанные сообщения обработаны, завершение")
				return
			}
		}

		// Обрабатываем exception queue асинхронно, если включено (чтобы не блокировать основной цикл)
		if processExceptionQueue && exceptionQueueReader != nil {
			allHandlersWg.Add(1)
			go func() {
				defer allHandlersWg.Done()
				// Передаем контекст для возможности отмены операций при graceful shutdown
				exceptionMessages, err := exceptionQueueReader.DequeueMany(ctx, 100)
				if err != nil {
					// Проверяем, была ли ошибка из-за отмены контекста (graceful shutdown)
					if ctx.Err() == context.Canceled {
						logger.Log.Info("Выборка сообщений из exception queue отменена из-за graceful shutdown")
						return
					}
					logger.Log.Error("Ошибка при выборке сообщений из exception queue", zap.Error(err))
					return
				}
				if len(exceptionMessages) > 0 {
					logger.Log.Info("Получено сообщений из exception queue", zap.Int("count", len(exceptionMessages)))

					// ВАЖНО: Сообщения уже вычитаны из exception queue, поэтому мы ОБЯЗАНЫ обработать их до конца
					for i, msg := range exceptionMessages {
						logger.Log.Debug("Обработка сообщения из exception queue",
							zap.Int("messageNum", i+1),
							zap.String("messageID", msg.MessageID),
							zap.Time("dequeueTime", msg.DequeueTime))

						// Парсим XML сообщение
						parsed, err := exceptionQueueReader.ParseXMLMessage(msg)
						if err != nil {
							logger.Log.Error("Ошибка парсинга XML", zap.Error(err), zap.String("messageID", msg.MessageID))
							continue
						}

						// Выводим распарсенные данные в JSON формате для читаемости
						jsonData, _ := json.MarshalIndent(parsed, "", "  ")
						logger.Log.Debug("Распарсенные данные", zap.String("data", string(jsonData)))

						// Преобразуем распарсенные данные в SMSMessage
						smsMsg, err := sms.ParseSMSMessage(parsed)
						if err != nil {
							logger.Log.Error("Ошибка преобразования в SMSMessage", zap.Error(err))
							continue
						}

						// Обрабатываем и отправляем SMS из exception queue
						// Передаем контекст для возможности отмены при graceful shutdown
						response, err := smsService.ProcessSMS(ctx, *smsMsg)

						// Проверяем, было ли сообщение отложено (schedule=1 вне окна расписания)
						// В этом случае ProcessSMS возвращает nil, nil - сообщение будет отправлено позже
						if response == nil && err == nil {
							logger.Log.Debug("SMS из exception queue добавлено в очередь отложенных, пропускаем",
								zap.Int64("taskID", smsMsg.TaskID))
							continue
						}

						if err != nil {
							// Проверяем, была ли операция отменена из-за graceful shutdown
							if ctx.Err() == context.Canceled {
								logger.Log.Warn("Обработка SMS из exception queue отменена из-за graceful shutdown",
									zap.Int64("taskID", smsMsg.TaskID))
								// ВАЖНО: Сообщение уже вычитано из exception queue (REMOVE), поэтому ОБЯЗАНЫ сохранить статус в БД
								// даже если SMS не был отправлен, чтобы зафиксировать факт обработки
								// Используем batch-очередь для сохранения
								if response != nil {
									smsResponseQueue.Enqueue(response)
									logger.Log.Debug("Результат отмененного SMS из exception queue добавлен в очередь для сохранения",
										zap.Int64("taskID", response.TaskID))
								}
								continue
							}
							logger.Log.Error("Ошибка обработки SMS из exception queue", zap.Error(err))
							// Даже при ошибке сохраняем результат в БД (статус ошибки)
							// Используем batch-очередь для сохранения
							if response != nil {
								smsResponseQueue.Enqueue(response)
							}
							continue
						}

						// Логируем результат
						logger.Log.Info("Результат отправки SMS из exception queue",
							zap.Int64("taskID", response.TaskID),
							zap.Int("status", response.Status),
							zap.String("messageID", response.MessageID),
							zap.String("errorText", response.ErrorText))

						// Сохраняем результат в БД через batch-очередь
						// Batch-сохранение происходит асинхронно по 50 результатов или каждые 2 секунды
						smsResponseQueue.Enqueue(response)
					}
				}
			}()
		}

		// Пауза между циклами: 0.5 секунды (аналогично Python: time.sleep(settings.main_circle_pause))
		// где main_circle_pause = 0.5 секунд
		// Используем select для возможности прерывания во время задержки
		if !sleepWithContext(ctx, 500*time.Millisecond) {
			return
		}
	}
}

// sleepWithContext выполняет задержку с возможностью прерывания через контекст
// Возвращает false, если контекст был отменен
func sleepWithContext(ctx context.Context, duration time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(duration):
		return true
	}
}

// performGracefulShutdown выполняет корректное завершение всех операций
// Принимает контекст с таймаутом для контроля времени завершения и единый таймаут shutdown
func performGracefulShutdown(ctx context.Context, smsService *sms.Service, dbConn *db.DBConnection, allHandlersWg *sync.WaitGroup, shutdownTimeout time.Duration, smsResponseQueue *sms.SmsResponseQueue) {
	logger.Log.Info("Завершение graceful shutdown...")

	// Проверяем, есть ли еще активные операции
	activeOps := dbConn.GetActiveOperationsCount()
	if activeOps > 0 {
		logger.Log.Info("Ожидание завершения активных операций с БД",
			zap.Int32("activeOperations", activeOps))
		// Даем дополнительное время на завершение операций с БД
		// Используем единый таймаут shutdown
		checkCtx, checkCancel := context.WithTimeout(ctx, shutdownTimeout)
		defer checkCancel()

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-checkCtx.Done():
				logger.Log.Warn("Таймаут ожидания активных операций истек",
					zap.Int32("remainingOperations", dbConn.GetActiveOperationsCount()))
				goto shutdown
			case <-ticker.C:
				if dbConn.GetActiveOperationsCount() == 0 {
					logger.Log.Info("Все активные операции с БД завершены")
					goto shutdown
				}
			}
		}
	}

shutdown:
	logger.Log.Info("Ожидание завершения всех обработчиков сообщений...")
	// Используем канал для неблокирующего ожидания
	done := make(chan struct{})
	go func() {
		allHandlersWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Log.Info("Все обработчики сообщений завершены")
	case <-ctx.Done():
		logger.Log.Warn("Таймаут ожидания обработчиков истек, принудительное завершение")
	}

	logger.Log.Info("Остановка механизма периодического переподключения SMPP...")
	smsService.StopPeriodicRebind()

	logger.Log.Info("Остановка механизма повторных попыток отправки SMS...")
	smsService.StopRetryWorker()

	logger.Log.Info("Остановка очереди отложенных SMS...",
		zap.Int("scheduledQueueSize", smsService.GetScheduledQueueSize()))
	smsService.StopScheduledQueue()

	logger.Log.Info("Ожидание завершения обработки delivery receipts...",
		zap.Duration("timeout", shutdownTimeout),
		zap.Int("queueSize", smsService.GetDeliveryReceiptQueueSize()))
	if !smsService.WaitForAllDeliveryReceipts(shutdownTimeout) {
		logger.Log.Warn("Таймаут ожидания delivery receipts истек, некоторые receipts могут быть потеряны",
			zap.Int("remainingInQueue", smsService.GetDeliveryReceiptQueueSize()))
	}

	logger.Log.Info("Остановка очереди результатов SMS...",
		zap.Int("queueSize", smsResponseQueue.GetQueueSize()))
	smsResponseQueue.Stop()

	logger.Log.Info("Закрытие SMPP адаптеров...")
	if err := smsService.Close(); err != nil {
		logger.Log.Error("Ошибка при закрытии SMPP адаптеров", zap.Error(err))
	}

	logger.Log.Info("Остановка механизма периодического переподключения к БД...")
	dbConn.StopPeriodicReconnect()

	logger.Log.Info("Graceful shutdown завершен успешно")
}
