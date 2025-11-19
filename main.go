package main

import (
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

	// Пример выполнения простого запроса
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
	log.Println("Список всех таблиц в базе данных:")

	// Запрос для получения списка всех таблиц
	tablesQuery := `
		SELECT 
			owner,
			table_name,
			tablespace_name,
			num_rows
		FROM all_tables 
		WHERE owner NOT IN ('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WMSYS')
		ORDER BY owner, table_name
	`

	tables, err := dbConn.ExecuteQuery(tablesQuery)
	if err != nil {
		log.Printf("Ошибка выполнения запроса списка таблиц: %v", err)
		return
	}

	log.Printf("Найдено таблиц: %d", len(tables))
	for i, table := range tables {
		owner := table["OWNER"]
		tableName := table["TABLE_NAME"]
		tablespace := table["TABLESPACE_NAME"]
		numRows := table["NUM_ROWS"]
		log.Printf("%d. [%s].%s (tablespace: %s, rows: %v)",
			i+1, owner, tableName, tablespace, numRows)
	}
}
