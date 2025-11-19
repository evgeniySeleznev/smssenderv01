package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/godror/godror"
	"gopkg.in/ini.v1"
)

// DBConnection — аналог Python-класса, инкапсулирует соединение и операции с БД.
type DBConnection struct {
	cfg       *ini.File
	db        *sql.DB
	ctx       context.Context
	cancel    context.CancelFunc
	lastError error
}

// NewDBConnection загружает конфигурацию из settings/db_settings.ini.
// Ожидаются секция [main] и ключи: username, password, dsn, username_write, password_write.
func NewDBConnection() (*DBConnection, error) {
	settingsPath := "./settings/db_settings.ini"
	if _, err := os.Stat(settingsPath); os.IsNotExist(err) {
		log.Printf("файл настроек не найден: %s", settingsPath)
		return nil, fmt.Errorf("файл настроек не найден: %s", settingsPath)
	}

	cfg, err := ini.Load(settingsPath)
	if err != nil {
		log.Printf("не удалось прочитать файл настроек %s: %v", settingsPath, err)
		return nil, fmt.Errorf("не удалось прочитать файл настроек %s: %w", settingsPath, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &DBConnection{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// OpenConnection открывает подключение к Oracle через драйвер godror (Oracle Instant Client).
func (d *DBConnection) OpenConnection() error {
	sec := d.cfg.Section("main")

	user := sec.Key("username").String()
	pass := sec.Key("password").String()
	if pass == "" { // совместимость со старым опечатанным ключом
		pass = sec.Key("passwword").String()
	}
	dsn := sec.Key("dsn").String()

	// Формируем строку подключения для godror
	// Godror использует формат: user/password@connectString
	var connString string

	// Обрабатываем DSN - может быть TNS DESCRIPTION или Easy Connect
	if len(dsn) > 0 && dsn[0] == '(' {
		// TNS DESCRIPTION - используем как есть
		connString = fmt.Sprintf("%s/%s@%s", user, pass, dsn)
	} else {
		// Easy Connect формат или TNS alias
		connString = fmt.Sprintf("%s/%s@%s", user, pass, dsn)
	}

	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Printf("ошибка sql.Open: %v", err)
		return fmt.Errorf("ошибка sql.Open: %w", err)
	}

	// Базовые настройки пула
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Проверка соединения
	if err := db.PingContext(d.ctx); err != nil {
		_ = db.Close()
		log.Printf("ошибка ping: %v", err)
		return fmt.Errorf("ошибка ping: %w", err)
	}
	d.db = db
	log.Println("Database connection opened (using Oracle Instant Client via godror).")
	return nil
}

// Commit — транзакционный commit для явной транзакции; в database/sql это делается через *sql.Tx.
// Чтобы повторить поведение Python-класса, дадим вспомогательный метод, который принимает *sql.Tx.
func (d *DBConnection) Commit(tx *sql.Tx) error {
	if tx == nil {
		return errors.New("tx == nil")
	}
	return tx.Commit()
}

// CloseConnection закрывает соединение.
func (d *DBConnection) CloseConnection() {
	if d.cancel != nil {
		d.cancel()
	}
	if d.db != nil {
		_ = d.db.Close()
	}
	log.Println("Database connection closed.")
}

// CallFunction вызывает хранимую функцию.
// Если функция возвращает SYS_REFCURSOR — вернёт []map[string]any с данными курсора.
// Иначе вернёт значение как interface{}.
func (d *DBConnection) CallFunction(functionName string, returnIsRefCursor bool, params []any) (any, error) {
	if d.db == nil {
		return nil, errors.New("соединение не открыто")
	}

	// Формируем PL/SQL-блок: :ret := schema.pkg.func(:1, :2, ...)
	// Если функция в пакете — используйте полное имя "SCHEMA.PKG.FUNC".
	placeholders := make([]string, 0, len(params))
	for i := range params {
		placeholders = append(placeholders, fmt.Sprintf(":%d", i+1))
	}
	plsql := fmt.Sprintf("begin :ret := %s(%s); end;", functionName, joinWithComma(placeholders))

	args := make([]any, 0, len(params)+1)

	if returnIsRefCursor {
		// Возврат рефкурсора через sql.Out с Dest типа driver.Rows
		var r driver.Rows
		args = append(args, sql.Out{Dest: &r})
		for _, p := range params {
			args = append(args, p)
		}
		_, err := d.db.ExecContext(d.ctx, plsql, args...)
		if err != nil {
			log.Printf("Database error calling %s: %v", functionName, err)
			return nil, err
		}
		defer func() { _ = r.Close() }()

		// Преобразуем курсор в слайс map-ов
		rows, err := readRefCursor(r)
		if err != nil {
			log.Printf("Ошибка чтения рефкурсора: %v", err)
			return nil, err
		}
		return rows, nil
	}

	// Не курсор: вернём значение как interface{}
	var ret any
	args = append(args, sql.Out{Dest: &ret})
	for _, p := range params {
		args = append(args, p)
	}

	if _, err := d.db.ExecContext(d.ctx, plsql, args...); err != nil {
		log.Printf("Database error calling %s: %v", functionName, err)
		return nil, err
	}
	return ret, nil
}

// CallProcedure вызывает процедуру с позиционными параметрами.
func (d *DBConnection) CallProcedure(procName string, params []any) error {
	if d.db == nil {
		return errors.New("соединение не открыто")
	}
	placeholders := make([]string, 0, len(params))
	for i := range params {
		placeholders = append(placeholders, fmt.Sprintf(":%d", i+1))
	}
	plsql := fmt.Sprintf("begin %s(%s); end;", procName, joinWithComma(placeholders))

	if _, err := d.db.ExecContext(d.ctx, plsql, params...); err != nil {
		return fmt.Errorf("ошибка вызова процедуры '%s': %w", procName, err)
	}
	return nil
}

// ExecuteQuery выполняет произвольный запрос с параметрами и возвращает результат.
func (d *DBConnection) ExecuteQuery(query string, args ...any) ([]map[string]any, error) {
	if d.db == nil {
		return nil, errors.New("соединение не открыто")
	}
	log.Printf("execute_query: %s", query)
	rows, err := d.db.QueryContext(d.ctx, query, args...)
	if err != nil {
		log.Printf("Ошибка при выполнении запроса: %v", err)
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for rows.Next() {
		holders := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range holders {
			ptrs[i] = &holders[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rec := make(map[string]any, len(cols))
		for i, c := range cols {
			rec[c] = jsonSafe(holders[i])
		}
		results = append(results, rec)
		log.Printf("row: %+v", rec)
	}
	return results, rows.Err()
}

// readRefCursor читает строки из driver.Rows (SYS_REFCURSOR) и возвращает []map[string]any.
// Паттерн работы с REFCURSOR через sql.Out{Dest: &driver.Rows} описан в godror [1][2].
func readRefCursor(r driver.Rows) ([]map[string]any, error) {
	cols := r.Columns()
	var out []map[string]any

	for {
		dest := make([]driver.Value, len(cols))
		if err := r.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = jsonSafe(dest[i])
		}
		out = append(out, row)
	}
	return out, nil
}

// jsonSafe — упрощённая версия "безопасного" приведения типов под JSON/лог.
// - nil -> nil
// - []byte -> base64
// - time.Time -> ISO8601
// - числа/строки/булевы — как есть
// - прочие — fmt.Sprint
func jsonSafe(v any) any {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []byte:
		// BLOB или RAW -> base64
		return base64.StdEncoding.EncodeToString(t)
	case time.Time:
		return t.Format(time.RFC3339Nano)
	case string, int64, float64, bool:
		return t
	default:
		// Некоторые числовые/decimal типы могут приходить как godror.Number (string-представление).
		// LOB CLOB обычно маппится как string при Scan.
		return fmt.Sprint(v)
	}
}

// parseTNSDescription извлекает хост, порт и SERVICE_NAME из TNS DESCRIPTION строки.
func parseTNSDescription(tns string) (host, port, serviceName string) {
	// Простой парсинг TNS DESCRIPTION формата:
	// (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = ...)(PORT = ...))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = ...)))

	// Извлекаем HOST
	if i := strings.Index(tns, "HOST = "); i >= 0 {
		start := i + len("HOST = ")
		end := strings.Index(tns[start:], ")")
		if end > 0 {
			host = strings.TrimSpace(tns[start : start+end])
		}
	}

	// Извлекаем PORT
	if i := strings.Index(tns, "PORT = "); i >= 0 {
		start := i + len("PORT = ")
		end := strings.Index(tns[start:], ")")
		if end > 0 {
			port = strings.TrimSpace(tns[start : start+end])
		}
	}

	// Извлекаем SERVICE_NAME
	if i := strings.Index(tns, "SERVICE_NAME = "); i >= 0 {
		start := i + len("SERVICE_NAME = ")
		end := strings.Index(tns[start:], ")")
		if end > 0 {
			serviceName = strings.TrimSpace(tns[start : start+end])
		}
	}

	return host, port, serviceName
}

func joinWithComma(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	default:
		out := parts[0]
		for _, p := range parts[1:] {
			out += ", " + p
		}
		return out
	}
}

// PrintParamTypes — аналог print_param_types из Python.
func PrintParamTypes(params []any) {
	for i, p := range params {
		log.Printf("Param %d: Type=%T, Value=%v", i, p, p)
	}
}

// GetDB возвращает *sql.DB для прямого доступа (если необходимо).
func (d *DBConnection) GetDB() *sql.DB {
	return d.db
}

// GetContext возвращает контекст подключения.
func (d *DBConnection) GetContext() context.Context {
	return d.ctx
}
