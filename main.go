package main

import (
	"context"
	"encoding/json"
	"log"
	"oracle-client/db"
	"oracle-client/sms"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Создаем контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Настраиваем обработку сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	// Обрабатываем основные сигналы, которые работают на всех платформах
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	// Также обрабатываем SIGHUP на Unix системах (на Windows не поддерживается)
	if runtime.GOOS != "windows" {
		signal.Notify(sigChan, syscall.SIGHUP)
	}

	// Горутина для обработки сигналов
	go func() {
		sig := <-sigChan
		log.Printf("Получен сигнал %v, инициируется graceful shutdown...", sig)
		cancel()
	}()

	// Создаем подключение к БД
	dbConn, err := db.NewDBConnection()
	if err != nil {
		log.Fatalf("Ошибка создания подключения: %v", err)
	}
	defer dbConn.CloseConnection()

	// Открываем соединение
	if err := dbConn.OpenConnection(); err != nil {
		log.Fatalf("Ошибка открытия соединения: %v", err)
	}

	log.Println("Успешно подключено к Oracle базе данных")

	// Запускаем механизм периодического переподключения к БД (каждые 300 секунд)
	dbConn.StartPeriodicReconnect()

	// Загружаем конфигурацию SMS сервиса
	smsConfig, err := sms.LoadConfig(dbConn.GetConfig())
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации SMS: %v", err)
	}
	log.Printf("Конфигурация SMS загружена. Режим Silent: %v", smsConfig.Mode.Silent)

	// Создаем SMS сервис
	smsService := sms.NewService(smsConfig)

	// Устанавливаем функцию получения тестового номера для режима Debug
	smsService.SetTestPhoneGetter(dbConn.GetTestPhone)

	// Инициализируем SMPP адаптеры заранее (подключаемся ко всем провайдерам)
	if err := smsService.InitializeAdapters(); err != nil {
		log.Printf("Предупреждение: ошибка инициализации адаптеров: %v", err)
		log.Println("Адаптеры будут инициализированы при первой отправке SMS")
	}

	// Создаем QueueReader
	queueReader, err := db.NewQueueReader(dbConn)
	if err != nil {
		log.Fatalf("Ошибка создания QueueReader: %v", err)
	}

	log.Printf("Очередь: %s", queueReader.GetQueueName())
	log.Printf("Consumer: %s", queueReader.GetConsumerName())
	log.Println("Начало работы с очередью Oracle AQ...")

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
			log.Printf("Предупреждение: не удалось создать ExceptionQueueReader: %v", err)
			log.Println("Продолжаем работу без обработки exception queue")
			processExceptionQueue = false
		} else {
			log.Printf("Exception Queue: %s", exceptionQueueReader.GetQueueName())
			log.Println("Обработка exception queue включена")
			exceptionQueueReader.SetWaitTimeout(25)
		}
	}

	// Функция для асинхронной обработки батча сообщений
	// ВАЖНО: Сообщения уже вычитаны из очереди Oracle, поэтому мы ОБЯЗАНЫ обработать их до конца,
	// иначе они будут потеряны. При graceful shutdown мы прекращаем только чтение новых сообщений,
	// но продолжаем обрабатывать уже вычитанные.
	processMessagesBatch := func(ctx context.Context, messages []*db.QueueMessage, batchNum int) {
		log.Printf("Обработка батча %d: %d сообщений", batchNum, len(messages))
		for i, msg := range messages {
			// НЕ прерываем обработку уже вычитанных сообщений - они должны быть обработаны до конца
			// Проверка контекста здесь только для логирования, но не для прерывания

			log.Printf("\n--- Сообщение %d (батч %d) ---", i+1, batchNum)
			log.Printf("MessageID: %s", msg.MessageID)
			log.Printf("DequeueTime: %s", msg.DequeueTime.Format("2006-01-02 15:04:05"))

			// Парсим XML сообщение
			parsed, err := queueReader.ParseXMLMessage(msg)
			if err != nil {
				log.Printf("Ошибка парсинга XML: %v", err)
				continue
			}

			// Выводим распарсенные данные в JSON формате для читаемости
			jsonData, _ := json.MarshalIndent(parsed, "", "  ")
			log.Printf("Распарсенные данные:\n%s", string(jsonData))

			// Преобразуем распарсенные данные в SMSMessage
			smsMsg, err := sms.ParseSMSMessage(parsed)
			if err != nil {
				log.Printf("Ошибка преобразования в SMSMessage: %v", err)
				continue
			}

			// Обрабатываем и отправляем SMS
			response, err := smsService.ProcessSMS(*smsMsg)
			if err != nil {
				log.Printf("Ошибка обработки SMS: %v", err)
				continue
			}

			// Логируем результат
			log.Printf("Результат отправки SMS: TaskID=%d, Status=%d, MessageID=%s, ErrorText=%s",
				response.TaskID, response.Status, response.MessageID, response.ErrorText)

			// Задержка между обработкой сообщений (аналогично C#: Thread.Sleep(20))
			// НЕ прерываем задержку - сообщения уже вычитаны из очереди и должны быть обработаны
			time.Sleep(20 * time.Millisecond)
		}
		log.Printf("Батч %d обработан", batchNum)
	}

	// WaitGroup для отслеживания всех обработчиков сообщений
	var allHandlersWg sync.WaitGroup

	// Запускаем основной цикл обработки очереди
	runQueueProcessingLoop(ctx, cancel, queueReader, exceptionQueueReader, smsService, dbConn, processExceptionQueue, &allHandlersWg, processMessagesBatch)

	// Graceful shutdown: дожидаемся завершения всех операций
	performGracefulShutdown(smsService, dbConn, &allHandlersWg)
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
			log.Println("Получен сигнал остановки, прекращаем чтение из очереди...")
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
				log.Println("Таймаут: не получен ответ из DequeueMany в течение 2 минут, завершаем работу...")
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
			log.Println("Нет доступных SMPP соединений, чтение очереди пропущено")
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
			log.Printf("Ошибка при выборке сообщений: %v", err)
			// При ошибке - переподключение
			log.Println("Ошибка соединения, переподключение...")
			if !sleepWithContext(ctx, 5*time.Second) {
				return
			}
			if err := dbConn.Reconnect(); err != nil {
				log.Printf("Ошибка переподключения: %v", err)
				if !sleepWithContext(ctx, 5*time.Second) {
					return
				}
			}
			continue
		}

		if len(messages) == 0 {
			// Очередь пуста - логируем и продолжаем цикл
			// Аналогично Python: logging.info(f"Очередь {self.connType} пуста в течение {settings.query_wait_time} секунд, перезапускаю слушатель")
			log.Println("Очередь пуста, ожидание следующей попытки...")
		} else {
			// Обрабатываем полученные сообщения асинхронно батчами по 100
			log.Printf("Получено сообщений: %d", len(messages))

			// Разбиваем на батчи по 10 сообщений для асинхронной обработки
			batchSize := 10
			var wg sync.WaitGroup

			for i := 0; i < len(messages); i += batchSize {
				// Проверяем контекст перед запуском нового батча
				if ctx.Err() != nil {
					log.Printf("Получен сигнал остановки, прекращаем запуск новых батчей (обработано %d/%d сообщений)", i, len(messages))
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
			log.Println("Все батчи обработаны")
		}

		// Обрабатываем exception queue асинхронно, если включено (чтобы не блокировать основной цикл)
		if processExceptionQueue && exceptionQueueReader != nil {
			allHandlersWg.Add(1)
			go func() {
				defer allHandlersWg.Done()
				// Передаем контекст для возможности отмены операций при graceful shutdown
				exceptionMessages, err := exceptionQueueReader.DequeueMany(ctx, 100)
				if err != nil {
					log.Printf("Ошибка при выборке сообщений из exception queue: %v", err)
					return
				}
				if len(exceptionMessages) > 0 {
					log.Printf("Получено сообщений из exception queue: %d", len(exceptionMessages))

					// ВАЖНО: Сообщения уже вычитаны из exception queue, поэтому мы ОБЯЗАНЫ обработать их до конца
					for i, msg := range exceptionMessages {

						log.Printf("\n--- Exception Queue Сообщение %d ---", i+1)
						log.Printf("MessageID: %s", msg.MessageID)
						log.Printf("DequeueTime: %s", msg.DequeueTime.Format("2006-01-02 15:04:05"))

						// Парсим XML сообщение
						parsed, err := exceptionQueueReader.ParseXMLMessage(msg)
						if err != nil {
							log.Printf("Ошибка парсинга XML: %v", err)
							continue
						}

						// Выводим распарсенные данные в JSON формате для читаемости
						jsonData, _ := json.MarshalIndent(parsed, "", "  ")
						log.Printf("Распарсенные данные:\n%s", string(jsonData))

						// Преобразуем распарсенные данные в SMSMessage
						smsMsg, err := sms.ParseSMSMessage(parsed)
						if err != nil {
							log.Printf("Ошибка преобразования в SMSMessage: %v", err)
							continue
						}

						// Обрабатываем и отправляем SMS из exception queue
						response, err := smsService.ProcessSMS(*smsMsg)
						if err != nil {
							log.Printf("Ошибка обработки SMS из exception queue: %v", err)
							continue
						}

						// Логируем результат
						log.Printf("Результат отправки SMS из exception queue: TaskID=%d, Status=%d, MessageID=%s, ErrorText=%s",
							response.TaskID, response.Status, response.MessageID, response.ErrorText)
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
func performGracefulShutdown(smsService *sms.Service, dbConn *db.DBConnection, allHandlersWg *sync.WaitGroup) {
	log.Println("Начало graceful shutdown...")
	log.Println("Ожидание завершения всех обработчиков сообщений...")
	allHandlersWg.Wait()
	log.Println("Все обработчики сообщений завершены")

	log.Println("Остановка механизма периодического переподключения SMPP...")
	smsService.StopPeriodicRebind()

	log.Println("Остановка механизма повторных попыток отправки SMS...")
	smsService.StopRetryWorker()

	log.Println("Закрытие SMPP адаптеров...")
	if err := smsService.Close(); err != nil {
		log.Printf("Ошибка при закрытии SMPP адаптеров: %v", err)
	}

	log.Println("Остановка механизма периодического переподключения к БД...")
	dbConn.StopPeriodicReconnect()

	log.Println("Graceful shutdown завершен успешно")
}
