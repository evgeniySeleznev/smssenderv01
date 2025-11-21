package main

import (
	"encoding/json"
	"log"
	"oracle-client/db"
	"oracle-client/sms"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
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

	// Загружаем конфигурацию SMS сервиса
	smsConfig, err := sms.LoadConfig(dbConn.GetConfig())
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации SMS: %v", err)
	}
	log.Printf("Конфигурация SMS загружена. Режим Silent: %v", smsConfig.Mode.Silent)

	// Создаем SMS сервис
	smsService := sms.NewService(smsConfig)

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
	processMessagesBatch := func(messages []*db.QueueMessage, batchNum int) {
		log.Printf("Обработка батча %d: %d сообщений", batchNum, len(messages))
		for i, msg := range messages {
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
		}
		log.Printf("Батч %d обработан", batchNum)
	}

	// Счетчик итераций для профилактического пересоздания пула
	iterationCount := 0
	const poolRecreateInterval = 100 // Пересоздание пула каждые 100 итераций

	// WaitGroup для отслеживания всех обработчиков сообщений
	var allHandlersWg sync.WaitGroup
	// Флаг для отслеживания необходимости завершения
	var shouldExit bool
	var exitMu sync.Mutex

	// Бесконечный цикл чтения из очереди (аналогично Python: while True)
	for {
		iterationCount++

		// Профилактическое пересоздание пула каждые 100 итераций
		if iterationCount > poolRecreateInterval {
			iterationCount = 0
			log.Println("Профилактическое пересоздание пула соединений...")
			dbConn.ClosePool()
			time.Sleep(5 * time.Second)
			if err := dbConn.RecreatePool(); err != nil {
				log.Printf("Ошибка пересоздания пула: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Println("Пул соединений пересоздан успешно")
		}

		// Проверка соединения перед каждым чтением (аналогично Python: connection.ping())
		if !dbConn.CheckConnection() {
			log.Println("Ошибка соединения, переподключение...")
			dbConn.ClosePool()
			time.Sleep(5 * time.Second)
			if err := dbConn.RecreatePool(); err != nil {
				log.Printf("Ошибка переподключения: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Println("Переподключение выполнено успешно")
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
				exitMu.Lock()
				shouldExit = true
				exitMu.Unlock()
			case <-iterationSignalChan:
				// Получили сигнал - ответ получен, прекращаем работу горутины
				return
			}
		}()

		// Получаем сообщения из основной очереди (аналогично Python: messages = queue.deqmany(settings.query_number))
		// Используем DequeueMany с количеством сообщений (аналогично settings.query_number = 100)
		messages, err := queueReader.DequeueMany(100)

		// Отправляем сигнал в горутину о получении ответа (независимо от результата)
		close(iterationSignalChan)
		if err != nil {
			log.Printf("Ошибка при выборке сообщений: %v", err)
			// При ошибке - очистка пула, пауза 5 сек и пересоздание пула
			log.Println("Ошибка соединения, переподключение...")
			dbConn.ClosePool()
			time.Sleep(5 * time.Second)
			if err := dbConn.RecreatePool(); err != nil {
				log.Printf("Ошибка переподключения: %v", err)
				time.Sleep(5 * time.Second)
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
					processMessagesBatch(batch, num)
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
				exceptionMessages, err := exceptionQueueReader.DequeueMany(100)
				if err != nil {
					log.Printf("Ошибка при выборке сообщений из exception queue: %v", err)
					return
				}
				if len(exceptionMessages) > 0 {
					log.Printf("Получено сообщений из exception queue: %d", len(exceptionMessages))

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

		// Проверяем флаг завершения
		exitMu.Lock()
		if shouldExit {
			exitMu.Unlock()
			log.Println("Ожидание завершения всех обработчиков сообщений...")
			allHandlersWg.Wait()
			log.Println("Все обработчики завершены, выход из приложения")
			return
		}
		exitMu.Unlock()

		// Пауза между циклами: 0.5 секунды (аналогично Python: time.sleep(settings.main_circle_pause))
		// где main_circle_pause = 0.5 секунд
		time.Sleep(500 * time.Millisecond)
	}
}
