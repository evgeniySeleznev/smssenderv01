package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/godror/godror"
	"gopkg.in/ini.v1"
)

// DBConnection — аналог Python-класса, инкапсулирует соединение и операции с БД.
type DBConnection struct {
	cfg               *ini.File
	db                *sql.DB
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.Mutex // Блокировка для потокобезопасного доступа к БД
	reconnectTicker   *time.Ticker
	reconnectStop     chan struct{}
	reconnectWg       sync.WaitGroup
	lastReconnect     time.Time
	reconnectInterval time.Duration // Интервал переподключения (300 секунд)
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
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		reconnectInterval: 300 * time.Second, // 300 секунд по умолчанию
		reconnectStop:     make(chan struct{}),
		lastReconnect:     time.Now(),
	}, nil
}

// OpenConnection открывает подключение к Oracle через драйвер godror (Oracle Instant Client).
func (d *DBConnection) OpenConnection() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.openConnectionInternal()
}

// CloseConnection закрывает соединение.
func (d *DBConnection) CloseConnection() {
	// Останавливаем периодическое переподключение
	d.StopPeriodicReconnect()

	if d.cancel != nil {
		d.cancel()
	}
	if d.db != nil {
		_ = d.db.Close()
		d.db = nil
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

// Reconnect переподключается к базе данных с пересозданием пула соединений
func (d *DBConnection) Reconnect() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	log.Println("Начало переподключения к базе данных...")

	// Закрываем текущий пул соединений
	if d.db != nil {
		_ = d.db.Close()
		d.db = nil
		log.Println("Текущий пул соединений закрыт.")
	}

	// Пересоздаем подключение (открываем новый пул)
	// openConnectionInternal уже логирует открытие соединения
	if err := d.openConnectionInternal(); err != nil {
		return fmt.Errorf("ошибка переподключения: %w", err)
	}

	log.Printf("Переподключение к базе данных выполнено успешно. Пул соединений пересоздан.")
	return nil
}

// openConnectionInternal внутренний метод открытия соединения без блокировки
func (d *DBConnection) openConnectionInternal() error {
	sec := d.cfg.Section("main")

	user := sec.Key("username").String()
	pass := sec.Key("password").String()
	if pass == "" { // совместимость со старым опечатанным ключом
		pass = sec.Key("passwword").String()
	}
	dsn := sec.Key("dsn").String()

	// Формируем строку подключения для godror
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
	d.lastReconnect = time.Now()
	log.Println("Database connection opened (using Oracle Instant Client via godror).")
	return nil
}

// StartPeriodicReconnect запускает горутину для периодического переподключения к БД
// Переподключение происходит каждые 300 секунд (или значение reconnectInterval)
func (d *DBConnection) StartPeriodicReconnect() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Если уже запущено, не запускаем повторно
	if d.reconnectTicker != nil {
		return
	}

	d.reconnectTicker = time.NewTicker(d.reconnectInterval)
	d.reconnectWg.Add(1)

	go func() {
		defer d.reconnectWg.Done()
		defer d.reconnectTicker.Stop()

		log.Printf("Запущен механизм периодического переподключения к БД (интервал: %v)", d.reconnectInterval)

		for {
			select {
			case <-d.reconnectTicker.C:
				// Время переподключения
				log.Printf("Периодическое переподключение к БД (прошло %v с последнего переподключения)", time.Since(d.lastReconnect))
				if err := d.Reconnect(); err != nil {
					log.Printf("Ошибка периодического переподключения: %v", err)
					// Продолжаем работу, попробуем в следующий раз
				}

			case <-d.reconnectStop:
				log.Println("Остановка механизма периодического переподключения к БД")
				return

			case <-d.ctx.Done():
				log.Println("Контекст отменен, остановка механизма периодического переподключения к БД")
				return
			}
		}
	}()
}

// StopPeriodicReconnect останавливает механизм периодического переподключения
func (d *DBConnection) StopPeriodicReconnect() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.reconnectTicker != nil {
		close(d.reconnectStop)
		d.reconnectTicker.Stop()
		d.reconnectTicker = nil
		// Ждем завершения горутины
		d.reconnectWg.Wait()
		// Создаем новый канал для следующего запуска
		d.reconnectStop = make(chan struct{})
	}
}

// GetConfig возвращает конфигурацию для доступа к настройкам.
func (d *DBConnection) GetConfig() *ini.File {
	return d.cfg
}

// GetTestPhone получает тестовый номер телефона из Oracle через процедуру pcsystem.PKG_SMS.GET_TEST_PHONE()
// Аналог C# метода OracleAdapter.LockGetTestPhone
// Использует блокировку для потокобезопасности
func (d *DBConnection) GetTestPhone() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db == nil {
		return "", fmt.Errorf("соединение с БД не открыто")
	}

	// Проверяем соединение
	if !d.CheckConnection() {
		return "", fmt.Errorf("соединение с БД недоступно")
	}

	// Вызываем функцию Oracle: pcsystem.PKG_SMS.GET_TEST_PHONE()
	// Функция возвращает VARCHAR2(500) - тестовый номер телефона
	var testPhone string
	query := "SELECT pcsystem.PKG_SMS.GET_TEST_PHONE() FROM DUAL"

	err := d.db.QueryRowContext(d.ctx, query).Scan(&testPhone)
	if err != nil {
		log.Printf("Ошибка выполнения pcsystem.PKG_SMS.GET_TEST_PHONE(): %v", err)
		return "", fmt.Errorf("ошибка получения тестового номера: %w", err)
	}

	if testPhone == "" {
		return "", fmt.Errorf("ошибка получения тестового номера: номер пуст")
	}

	log.Printf("pcsystem.PKG_SMS.GET_TEST_PHONE() result: L_PHONE_NUMBER = %s", testPhone)
	return testPhone, nil
}
