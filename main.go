package main

import (
	"encoding/json"
	"log"
	"oracle-client/db"
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
			exceptionQueueReader.SetWaitTimeout(10)
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
			} else {
				// Выводим распарсенные данные в JSON формате для читаемости
				jsonData, _ := json.MarshalIndent(parsed, "", "  ")
				log.Printf("Распарсенные данные:\n%s", string(jsonData))
			}
		}
		log.Printf("Батч %d обработан", batchNum)
	}

	// Счетчик итераций для профилактического пересоздания пула
	iterationCount := 0
	const poolRecreateInterval = 100 // Пересоздание пула каждые 100 итераций

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

		// Получаем сообщения из основной очереди (аналогично Python: messages = queue.deqmany(settings.query_number))
		// Используем DequeueMany с количеством сообщений (аналогично settings.query_number = 100)
		messages, err := queueReader.DequeueMany(100)
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
				go func(batch []*db.QueueMessage, num int) {
					defer wg.Done()
					processMessagesBatch(batch, num)
				}(batch, batchNum)
			}

			// Ждем завершения обработки всех батчей
			wg.Wait()
			log.Println("Все батчи обработаны")
		}

		// Обрабатываем exception queue, если включено
		if processExceptionQueue && exceptionQueueReader != nil {
			exceptionMessages, err := exceptionQueueReader.DequeueMany(100)
			if err != nil {
				log.Printf("Ошибка при выборке сообщений из exception queue: %v", err)
			} else if len(exceptionMessages) > 0 {
				log.Printf("Получено сообщений из exception queue: %d", len(exceptionMessages))

				for i, msg := range exceptionMessages {
					log.Printf("\n--- Exception Queue Сообщение %d ---", i+1)
					log.Printf("MessageID: %s", msg.MessageID)
					log.Printf("DequeueTime: %s", msg.DequeueTime.Format("2006-01-02 15:04:05"))

					// Парсим XML сообщение
					parsed, err := exceptionQueueReader.ParseXMLMessage(msg)
					if err != nil {
						log.Printf("Ошибка парсинга XML: %v", err)
					} else {
						// Выводим распарсенные данные в JSON формате для читаемости
						jsonData, _ := json.MarshalIndent(parsed, "", "  ")
						log.Printf("Распарсенные данные:\n%s", string(jsonData))
					}
				}
			}
		}

		// Пауза между циклами: 0.5 секунды (аналогично Python: time.sleep(settings.main_circle_pause))
		// где main_circle_pause = 0.5 секунд
		time.Sleep(500 * time.Millisecond)
	}
}
