package main

import (
	"encoding/json"
	"fmt"
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
	log.Println("Проверка доступных очередей Oracle AQ для текущего пользователя")

	// SQL запрос для получения всех доступных очередей для текущего пользователя
	// ALL_QUEUES показывает все очереди, к которым у пользователя есть доступ
	queuesQuery := `
		SELECT 
			OWNER,
			NAME AS QUEUE_NAME,
			QUEUE_TABLE,
			QUEUE_TYPE,
			ENQUEUE_ENABLED,
			DEQUEUE_ENABLED,
			RETENTION
		FROM ALL_QUEUES
		ORDER BY OWNER, NAME
	`

	queuesResults, err := dbConn.ExecuteQuery(queuesQuery)
	if err != nil {
		log.Printf("Ошибка выполнения запроса на получение очередей: %v", err)
	} else {
		log.Printf("\nНайдено доступных очередей: %d\n", len(queuesResults))
		if len(queuesResults) > 0 {
			log.Println(strings.Repeat("-", 100))
			log.Printf("%-20s %-30s %-15s %-10s %-10s %-10s\n",
				"ВЛАДЕЛЕЦ", "ИМЯ ОЧЕРЕДИ", "ТИП", "ENQUEUE", "DEQUEUE", "RETENTION")
			log.Println(strings.Repeat("-", 100))
			for _, queue := range queuesResults {
				owner := getStringValue(queue["OWNER"])
				queueName := getStringValue(queue["QUEUE_NAME"])
				queueType := getStringValue(queue["QUEUE_TYPE"])
				enqueueEnabled := getStringValue(queue["ENQUEUE_ENABLED"])
				dequeueEnabled := getStringValue(queue["DEQUEUE_ENABLED"])
				retention := getStringValue(queue["RETENTION"])

				log.Printf("%-20s %-30s %-15s %-10s %-10s %-10s",
					owner, queueName, queueType, enqueueEnabled, dequeueEnabled, retention)
			}
			log.Println(strings.Repeat("-", 100))

			// Дополнительная информация о подписчиках (consumers)
			subscribersQuery := `
				SELECT 
					QS.QUEUE_OWNER,
					QS.QUEUE_NAME,
					QS.CONSUMER_NAME,
					QS.ADDRESS,
					QS.PROTOCOL
				FROM ALL_QUEUE_SUBSCRIBERS QS
				ORDER BY QS.QUEUE_OWNER, QS.QUEUE_NAME, QS.CONSUMER_NAME
			`

			subscribersResults, err := dbConn.ExecuteQuery(subscribersQuery)
			if err != nil {
				log.Printf("Ошибка получения информации о подписчиках: %v", err)
			} else if len(subscribersResults) > 0 {
				log.Printf("\nНайдено подписчиков (consumers): %d\n", len(subscribersResults))
				log.Println(strings.Repeat("-", 100))
				log.Printf("%-20s %-30s %-30s\n",
					"ВЛАДЕЛЕЦ/ОЧЕРЕДЬ", "CONSUMER", "ADDRESS")
				log.Println(strings.Repeat("-", 100))
				for _, sub := range subscribersResults {
					queueOwner := getStringValue(sub["QUEUE_OWNER"])
					queueName := getStringValue(sub["QUEUE_NAME"])
					consumerName := getStringValue(sub["CONSUMER_NAME"])
					address := getStringValue(sub["ADDRESS"])

					log.Printf("%-20s.%-30s %-30s %s",
						queueOwner, queueName, consumerName, address)
				}
				log.Println(strings.Repeat("-", 100))
			}
		} else {
			log.Println("Доступных очередей не найдено")
		}
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

// getStringValue безопасно преобразует значение в строку, обрабатывая nil
func getStringValue(v interface{}) string {
	if v == nil {
		return "<NULL>"
	}
	return fmt.Sprintf("%v", v)
}
