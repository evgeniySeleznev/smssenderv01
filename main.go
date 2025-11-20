package main

import (
	"encoding/json"
	"log"
	"oracle-client/db"
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

	// Бесконечный цикл чтения из очереди (аналогично Python: while True)
	for {
		// Получаем сообщения из очереди (аналогично Python: messages = queue.deqmany(settings.query_number))
		// Используем DequeueMany с количеством сообщений (аналогично settings.query_number = 50)
		messages, err := queueReader.DequeueMany(50)
		if err != nil {
			log.Printf("Ошибка при выборке сообщений: %v", err)
			// При ошибке ждем перед следующей попыткой
			time.Sleep(10 * time.Second)
			continue
		}

		if len(messages) == 0 {
			// Очередь пуста - ждем 10 секунд перед следующей попыткой
			// Аналогично Python: logging.info(f"Очередь {self.connType} пуста в течение {settings.query_wait_time} секунд, перезапускаю слушатель")
			log.Println("Очередь пуста, ожидание 10 секунд перед следующей попыткой...")
			time.Sleep(10 * time.Second)
			continue
		}

		// Обрабатываем полученные сообщения
		log.Printf("Получено сообщений: %d", len(messages))

		for i, msg := range messages {
			log.Printf("\n--- Сообщение %d ---", i+1)
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

		// Небольшая задержка между циклами (аналогично Python: time.sleep(settings.main_circle_pause))
		// где main_circle_pause = 0.5 секунд
		time.Sleep(500 * time.Millisecond)
	}
}
