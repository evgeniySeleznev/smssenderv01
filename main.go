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
	// Дает время на завершение отправки SMS и записи в БД
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

	// Инициализируем SMPP адаптеры заранее (подключаемся ко всем провайдерам)
	if err := smsService.InitializeAdapters(); err != nil {
		logger.Log.Warn("Ошибка инициализации адаптеров", zap.Error(err))
		logger.Log.Info("Адаптеры будут инициализированы при первой отправке SMS")
	}

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

	// Функция для асинхронной обработки батча сообщений
	// ВАЖНО: Сообщения уже вычитаны из очереди Oracle, поэтому мы ОБЯЗАНЫ обработать их до конца,
	// иначе они будут потеряны. При graceful shutdown мы прекращаем только чтение новых сообщений,
	// но продолжаем обрабатывать уже вычитанные.
	processMessagesBatch := func(ctx context.Context, messages []*db.QueueMessage, batchNum int) {
		logger.Log.Debug("Обработка батча", zap.Int("batchNum", batchNum), zap.Int("count", len(messages)))
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
			if err != nil {
				// Проверяем, была ли операция отменена из-за graceful shutdown
				if ctx.Err() == context.Canceled {
					logger.Log.Warn("Обработка SMS отменена из-за graceful shutdown",
						zap.Int64("taskID", smsMsg.TaskID))
					// ВАЖНО: Сообщение уже вычитано из очереди (REMOVE), поэтому ОБЯЗАНЫ сохранить статус в БД
					// даже если SMS не был отправлен, чтобы зафиксировать факт обработки
					// Это предотвращает потерю данных при graceful shutdown
					if response != nil {
						saveParams := db.SaveSmsResponseParams{
							TaskID:       response.TaskID,
							MessageID:    response.MessageID,
							StatusID:     response.Status,
							ResponseDate: response.SentAt,
							ErrorText:    response.ErrorText,
						}
						if success, saveErr := dbConn.SaveSmsResponse(ctx, saveParams); !success {
							if saveErr != nil {
								// Даже если контекст отменен, пытаемся сохранить статус
								// Используем background context для критически важной операции сохранения
								bgCtx := context.Background()
								if retrySuccess, retryErr := dbConn.SaveSmsResponse(bgCtx, saveParams); !retrySuccess {
									logger.Log.Error("КРИТИЧЕСКАЯ ОШИБКА: не удалось сохранить статус отмененного SMS в БД",
										zap.Int64("taskID", response.TaskID),
										zap.Error(retryErr))
								} else {
									logger.Log.Info("Статус отмененного SMS сохранен в БД (использован background context)",
										zap.Int64("taskID", response.TaskID))
								}
							}
						} else {
							logger.Log.Info("Статус отмененного SMS сохранен в БД",
								zap.Int64("taskID", response.TaskID),
								zap.String("errorText", response.ErrorText))
						}
					}
					continue
				}
				logger.Log.Error("Ошибка обработки SMS", zap.Error(err))
				// Даже при ошибке сохраняем результат в БД (статус ошибки)
				// Но только если это не отмена контекста
				if response != nil {
					saveParams := db.SaveSmsResponseParams{
						TaskID:       response.TaskID,
						MessageID:    response.MessageID,
						StatusID:     response.Status,
						ResponseDate: response.SentAt,
						ErrorText:    response.ErrorText,
					}
					if success, saveErr := dbConn.SaveSmsResponse(ctx, saveParams); !success {
						if saveErr != nil && ctx.Err() != context.Canceled {
							logger.Log.Error("Ошибка сохранения результата SMS в БД",
								zap.Int64("taskID", response.TaskID),
								zap.Error(saveErr))
						}
					}
				}
				continue
			}

			// Логируем результат
			logger.Log.Info("Результат отправки SMS",
				zap.Int64("taskID", response.TaskID),
				zap.Int("status", response.Status),
				zap.String("messageID", response.MessageID),
				zap.String("errorText", response.ErrorText))

			// Сохраняем результат в БД через процедуру save_sms_response
			// Передаем контекст для возможности отмены при graceful shutdown
			// Если SMS был успешно отправлен, пытаемся сохранить результат даже при отмене контекста
			// (так как SMS уже отправлен, нужно зафиксировать это в БД)
			saveParams := db.SaveSmsResponseParams{
				TaskID:       response.TaskID,
				MessageID:    response.MessageID,
				StatusID:     response.Status,
				ResponseDate: response.SentAt,
				ErrorText:    response.ErrorText,
			}
			if success, err := dbConn.SaveSmsResponse(ctx, saveParams); !success {
				if err != nil {
					// Проверяем, была ли операция отменена из-за graceful shutdown
					if ctx.Err() == context.Canceled {
						// Если SMS был отправлен, но сохранение отменено - это проблема
						// Логируем предупреждение, так как SMS уже отправлен, но не сохранен в БД
						logger.Log.Warn("Сохранение результата SMS отменено из-за graceful shutdown (SMS уже отправлен)",
							zap.Int64("taskID", response.TaskID),
							zap.String("messageID", response.MessageID))
					} else {
						logger.Log.Error("Ошибка сохранения результата SMS в БД",
							zap.Int64("taskID", response.TaskID),
							zap.Error(err))
					}
				}
			}

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
		runQueueProcessingLoop(ctx, cancel, queueReader, exceptionQueueReader, smsService, dbConn, processExceptionQueue, &allHandlersWg, processMessagesBatch)
	}()

	// Ждем сигнала shutdown
	<-shutdownRequested

	// Создаем контекст с таймаутом для завершения операций
	// Это дает дополнительное время на завершение отправки SMS и записи в БД
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	logger.Log.Info("Начало graceful shutdown с таймаутом",
		zap.Duration("timeout", shutdownTimeout))

	// Отменяем основной контекст, чтобы прекратить чтение новых сообщений
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
	performGracefulShutdown(shutdownCtx, smsService, dbConn, &allHandlersWg)
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
		if err != nil {
			logger.Log.Error("Ошибка при выборке сообщений", zap.Error(err))
			// При ошибке - переподключение
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
				// Проверяем контекст перед запуском нового батча
				if ctx.Err() != nil {
					logger.Log.Info("Получен сигнал остановки, прекращаем запуск новых батчей",
						zap.Int("processed", i),
						zap.Int("total", len(messages)))
					break
				}

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
		}

		// Обрабатываем exception queue асинхронно, если включено (чтобы не блокировать основной цикл)
		if processExceptionQueue && exceptionQueueReader != nil {
			allHandlersWg.Add(1)
			go func() {
				defer allHandlersWg.Done()
				// Передаем контекст для возможности отмены операций при graceful shutdown
				exceptionMessages, err := exceptionQueueReader.DequeueMany(ctx, 100)
				if err != nil {
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
						if err != nil {
							// Проверяем, была ли операция отменена из-за graceful shutdown
							if ctx.Err() == context.Canceled {
								logger.Log.Warn("Обработка SMS из exception queue отменена из-за graceful shutdown",
									zap.Int64("taskID", smsMsg.TaskID))
								// ВАЖНО: Сообщение уже вычитано из exception queue (REMOVE), поэтому ОБЯЗАНЫ сохранить статус в БД
								// даже если SMS не был отправлен, чтобы зафиксировать факт обработки
								if response != nil {
									saveParams := db.SaveSmsResponseParams{
										TaskID:       response.TaskID,
										MessageID:    response.MessageID,
										StatusID:     response.Status,
										ResponseDate: response.SentAt,
										ErrorText:    response.ErrorText,
									}
									if success, saveErr := dbConn.SaveSmsResponse(ctx, saveParams); !success {
										if saveErr != nil {
											// Даже если контекст отменен, пытаемся сохранить статус
											// Используем background context для критически важной операции сохранения
											bgCtx := context.Background()
											if retrySuccess, retryErr := dbConn.SaveSmsResponse(bgCtx, saveParams); !retrySuccess {
												logger.Log.Error("КРИТИЧЕСКАЯ ОШИБКА: не удалось сохранить статус отмененного SMS из exception queue в БД",
													zap.Int64("taskID", response.TaskID),
													zap.Error(retryErr))
											} else {
												logger.Log.Info("Статус отмененного SMS из exception queue сохранен в БД (использован background context)",
													zap.Int64("taskID", response.TaskID))
											}
										}
									} else {
										logger.Log.Info("Статус отмененного SMS из exception queue сохранен в БД",
											zap.Int64("taskID", response.TaskID),
											zap.String("errorText", response.ErrorText))
									}
								}
								continue
							}
							logger.Log.Error("Ошибка обработки SMS из exception queue", zap.Error(err))
							// Даже при ошибке сохраняем результат в БД (статус ошибки)
							// Но только если это не отмена контекста
							if response != nil {
								saveParams := db.SaveSmsResponseParams{
									TaskID:       response.TaskID,
									MessageID:    response.MessageID,
									StatusID:     response.Status,
									ResponseDate: response.SentAt,
									ErrorText:    response.ErrorText,
								}
								if success, saveErr := dbConn.SaveSmsResponse(ctx, saveParams); !success {
									if saveErr != nil && ctx.Err() != context.Canceled {
										logger.Log.Error("Ошибка сохранения результата SMS из exception queue в БД",
											zap.Int64("taskID", response.TaskID),
											zap.Error(saveErr))
									}
								}
							}
							continue
						}

						// Логируем результат
						logger.Log.Info("Результат отправки SMS из exception queue",
							zap.Int64("taskID", response.TaskID),
							zap.Int("status", response.Status),
							zap.String("messageID", response.MessageID),
							zap.String("errorText", response.ErrorText))

						// Сохраняем результат в БД через процедуру save_sms_response
						// Передаем контекст для возможности отмены при graceful shutdown
						// Если SMS был успешно отправлен, пытаемся сохранить результат даже при отмене контекста
						// (так как SMS уже отправлен, нужно зафиксировать это в БД)
						saveParams := db.SaveSmsResponseParams{
							TaskID:       response.TaskID,
							MessageID:    response.MessageID,
							StatusID:     response.Status,
							ResponseDate: response.SentAt,
							ErrorText:    response.ErrorText,
						}
						if success, err := dbConn.SaveSmsResponse(ctx, saveParams); !success {
							if err != nil {
								// Проверяем, была ли операция отменена из-за graceful shutdown
								if ctx.Err() == context.Canceled {
									// Если SMS был отправлен, но сохранение отменено - это проблема
									// Логируем предупреждение, так как SMS уже отправлен, но не сохранен в БД
									logger.Log.Warn("Сохранение результата SMS из exception queue отменено из-за graceful shutdown (SMS уже отправлен)",
										zap.Int64("taskID", response.TaskID),
										zap.String("messageID", response.MessageID))
								} else {
									logger.Log.Error("Ошибка сохранения результата SMS из exception queue в БД",
										zap.Int64("taskID", response.TaskID),
										zap.Error(err))
								}
							}
						}
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
// Принимает контекст с таймаутом для контроля времени завершения
func performGracefulShutdown(ctx context.Context, smsService *sms.Service, dbConn *db.DBConnection, allHandlersWg *sync.WaitGroup) {
	logger.Log.Info("Завершение graceful shutdown...")

	// Проверяем, есть ли еще активные операции
	activeOps := dbConn.GetActiveOperationsCount()
	if activeOps > 0 {
		logger.Log.Info("Ожидание завершения активных операций с БД",
			zap.Int32("activeOperations", activeOps))
		// Даем дополнительное время на завершение операций с БД
		// Используем короткий таймаут для проверки
		checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
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

	logger.Log.Info("Закрытие SMPP адаптеров...")
	if err := smsService.Close(); err != nil {
		logger.Log.Error("Ошибка при закрытии SMPP адаптеров", zap.Error(err))
	}

	logger.Log.Info("Остановка механизма периодического переподключения к БД...")
	dbConn.StopPeriodicReconnect()

	logger.Log.Info("Graceful shutdown завершен успешно")
}
