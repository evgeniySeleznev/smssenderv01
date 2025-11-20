package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/godror/godror"
	"gopkg.in/ini.v1"
)

// DBConnection — аналог Python-класса, инкапсулирует соединение и операции с БД.
type DBConnection struct {
	cfg    *ini.File
	db     *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
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
	connString := fmt.Sprintf("%s/%s@%s", user, pass, dsn)

	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Printf("ошибка sql.Open: %v", err)
		return fmt.Errorf("ошибка sql.Open: %w", err)
	}

	// Настройки пула согласно требованиям:
	// max=200, min=1 (по умолчанию), increment=10 (не поддерживается напрямую в sql.DB)
	// max_lifetime_session=300 секунд
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(10)                   // Аналог increment
	db.SetConnMaxLifetime(300 * time.Second) // 300 секунд = 5 минут
	db.SetConnMaxIdleTime(300 * time.Second)

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

// CheckConnection проверяет соединение с БД (аналогично Python: connection.ping())
func (d *DBConnection) CheckConnection() bool {
	if d.db == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.db.PingContext(ctx); err != nil {
		log.Printf("Ошибка проверки соединения: %v", err)
		return false
	}
	return true
}

// ClosePool закрывает пул соединений (для пересоздания)
func (d *DBConnection) ClosePool() {
	if d.db != nil {
		_ = d.db.Close()
		d.db = nil
		log.Println("Пул соединений закрыт.")
	}
}

// RecreatePool пересоздает пул соединений
func (d *DBConnection) RecreatePool() error {
	d.ClosePool()
	return d.OpenConnection()
}

// GetConfig возвращает конфигурацию для доступа к настройкам.
func (d *DBConnection) GetConfig() *ini.File {
	return d.cfg
}
