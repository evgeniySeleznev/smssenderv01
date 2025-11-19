package main

import (
	"encoding/json"
	"log"
	"oracle-client/db"
	"strings"
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

	// Проверка соединения простым запросом
	query := "SELECT sysdate FROM dual"
	results, err := dbConn.ExecuteQuery(query)
	if err != nil {
		log.Printf("Ошибка выполнения запроса: %v", err)
		return
	}

	log.Printf("Результаты запроса (%d строк):", len(results))
	for i, row := range results {
		log.Printf("Строка %d: %+v", i+1, row)
	}

	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("Работа с очередью Oracle AQ")

	// Создаем QueueReader
	queueReader, err := db.NewQueueReader(dbConn)
	if err != nil {
		log.Fatalf("Ошибка создания QueueReader: %v", err)
	}

	log.Printf("Очередь: %s", queueReader.GetQueueName())
	log.Printf("Consumer: %s", queueReader.GetConsumerName())

	// Вычитываем все сообщения из очереди
	log.Println("\nНачало выборки сообщений из очереди...")
	messages, err := queueReader.DequeueAll()
	if err != nil {
		log.Printf("Ошибка при выборке сообщений: %v", err)
		return
	}

	if len(messages) == 0 {
		log.Println("Очередь пуста, сообщений не найдено")
	} else {
		log.Printf("Получено сообщений: %d", len(messages))

		// Обрабатываем каждое сообщение
		for i, msg := range messages {
			log.Printf("\n--- Сообщение %d ---", i+1)
			log.Printf("MessageID: %s", msg.MessageID)
			log.Printf("DequeueTime: %s", msg.DequeueTime.Format("2006-01-02 15:04:05"))
			log.Printf("XML Payload (первые 200 символов): %s",
				truncateString(msg.XMLPayload, 200))

			// Парсим XML сообщение
			parsed, err := queueReader.ParseXMLMessage(msg)
			if err != nil {
				log.Printf("Ошибка парсинга XML: %v", err)
				log.Printf("Полный XML: %s", msg.XMLPayload)
			} else {
				// Выводим распарсенные данные в JSON формате для читаемости
				jsonData, _ := json.MarshalIndent(parsed, "", "  ")
				log.Printf("Распарсенные данные:\n%s", string(jsonData))
			}
		}
	}

	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("Работа с очередью завершена")
}

// truncateString обрезает строку до указанной длины
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
