package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/godror/godror"
	"gopkg.in/ini.v1"
)

const (
	// Таймауты для операций с БД
	pingTimeout       = 5 * time.Second  // Таймаут для проверки соединения
	queryTimeout      = 30 * time.Second // Таймаут для запросов
	execTimeout       = 30 * time.Second // Таймаут для выполнения команд
	connectionTimeout = 10 * time.Second // Таймаут для подключения
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
	reconnectInterval time.Duration // Интервал переподключения (30 минут)
	activeOps         atomic.Int32  // Счетчик активных операций с БД
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
		reconnectInterval: 30 * time.Minute, // 30 минут по умолчанию
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
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := d.db.PingContext(ctx); err != nil {
		log.Printf("Ошибка проверки соединения: %v", err)
		return false
	}
	return true
}

// Reconnect переподключается к базе данных с пересозданием пула соединений
// Проверяет наличие активных операций перед переподключением
// Использует безопасную блокировку без гонок
func (d *DBConnection) Reconnect() error {
	// Первая проверка: ждем завершения активных операций БЕЗ блокировки
	// Это позволяет операциям завершиться естественным образом
	if err := d.waitForActiveOperations(); err != nil {
		return fmt.Errorf("не удалось дождаться завершения активных операций: %w", err)
	}

	// Блокируем доступ к пулу для предотвращения новых операций
	// Используем цикл с проверкой для предотвращения гонок
	const maxRetries = 3
	const retryDelay = 50 * time.Millisecond

	// Пытаемся получить блокировку с проверкой активных операций
	for retry := 0; retry < maxRetries; retry++ {
		// Проверяем активные операции БЕЗ блокировки (быстрая проверка)
		activeCount := d.activeOps.Load()
		if activeCount == 0 {
			// Нет активных операций - получаем блокировку и переподключаемся
			break
		}

		if retry < maxRetries-1 {
			log.Printf("Обнаружены активные операции (%d операций), ожидание... (попытка %d/%d)",
				activeCount, retry+1, maxRetries)
			time.Sleep(retryDelay)
		} else {
			// Последняя попытка - ждем завершения операций
			log.Printf("Последняя попытка: ожидание завершения активных операций (%d операций)", activeCount)
			if err := d.waitForActiveOperations(); err != nil {
				return fmt.Errorf("не удалось дождаться завершения активных операций: %w", err)
			}
			// Финальная проверка
			activeCount = d.activeOps.Load()
			if activeCount > 0 {
				log.Printf("Предупреждение: переподключение выполняется при наличии активных операций (%d операций)", activeCount)
			}
			break
		}
	}

	// Получаем блокировку для переподключения
	// К этому моменту активные операции должны быть завершены
	d.mu.Lock()
	defer d.mu.Unlock()

	// Финальная проверка под блокировкой (на случай гонки)
	finalActiveCount := d.activeOps.Load()
	if finalActiveCount > 0 {
		log.Printf("Предупреждение: обнаружены активные операции под блокировкой (%d операций), продолжаем переподключение", finalActiveCount)
	}

	log.Println("Начало переподключения к базе данных...")

	// Закрываем текущий пул соединений
	if d.db != nil {
		_ = d.db.Close()
		d.db = nil
		log.Println("Текущий пул соединений закрыт.")
	}

	// Пересоздаем подключение (открываем новый пул)
	if err := d.openConnectionInternal(); err != nil {
		return fmt.Errorf("ошибка переподключения: %w", err)
	}

	log.Printf("Переподключение к базе данных выполнено успешно. Пул соединений пересоздан.")
	return nil
}

// waitForActiveOperations ждет завершения активных операций перед переподключением
// Максимальное время ожидания - 60 секунд
func (d *DBConnection) waitForActiveOperations() error {
	const maxWaitTime = 35 * time.Second
	const checkInterval = 100 * time.Millisecond

	startTime := time.Now()
	for {
		activeCount := d.activeOps.Load()
		if activeCount == 0 {
			return nil // Нет активных операций, можно переподключаться
		}

		// Проверяем, не превышен ли максимальный срок ожидания
		if time.Since(startTime) > maxWaitTime {
			log.Printf("Предупреждение: превышено время ожидания завершения активных операций (%d активных операций)", activeCount)
			// Продолжаем переподключение, но логируем предупреждение
			return nil
		}

		log.Printf("Ожидание завершения активных операций перед переподключением (активных операций: %d)", activeCount)
		time.Sleep(checkInterval)
	}
}

// BeginOperation отмечает начало операции с БД (увеличивает счетчик активных операций)
func (d *DBConnection) BeginOperation() {
	d.activeOps.Add(1)
}

// EndOperation отмечает завершение операции с БД (уменьшает счетчик активных операций)
func (d *DBConnection) EndOperation() {
	d.activeOps.Add(-1)
}

// GetActiveOperationsCount возвращает количество активных операций
func (d *DBConnection) GetActiveOperationsCount() int32 {
	return d.activeOps.Load()
}

// RecreatePool пересоздает пул соединений (алиас для Reconnect для обратной совместимости)
func (d *DBConnection) RecreatePool() error {
	return d.Reconnect()
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
	// max_lifetime_session синхронизирован с интервалом переподключения (30 минут)
	// Используем немного меньше времени (25 минут), чтобы соединения обновлялись постепенно
	// перед полным переподключением пула каждые 30 минут
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(10)                 // Аналог increment
	db.SetConnMaxLifetime(5 * time.Minute) // 5 минут - соединения обновляются постепенно
	db.SetConnMaxIdleTime(5 * time.Minute) // 5 минут для idle соединений

	// Проверка соединения с таймаутом
	pingCtx, pingCancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
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

	// Отмечаем начало операции с БД для предотвращения переподключения во время транзакции
	d.BeginOperation()
	defer d.EndOperation()

	// Создаем контекст с таймаутом для транзакции
	queryCtx, queryCancel := context.WithTimeout(context.Background(), queryTimeout)
	defer queryCancel()

	// Создаем временный пакет для хранения результата функции
	// Аналогично queue_reader.go - Oracle требует использования пакетных переменных
	if err := d.ensureTestPhonePackageExists(queryCtx); err != nil {
		return "", fmt.Errorf("ошибка создания пакета: %w", err)
	}

	// Выполняем операции в транзакции для обеспечения атомарности
	tx, err := d.db.BeginTx(queryCtx, nil)
	if err != nil {
		return "", fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback() // Откатываем, если что-то пойдет не так

	// Используем PL/SQL блок для вызова функции Oracle и сохранения результата в пакетную переменную
	// Аналогично queue_reader.go - Oracle требует использования PL/SQL блоков
	plsql := `
		BEGIN
			temp_test_phone_pkg.g_phone_number := pcsystem.PKG_SMS.GET_TEST_PHONE();
		END;
	`

	// Выполняем PL/SQL блок
	_, err = tx.ExecContext(queryCtx, plsql)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("Ошибка отката транзакции: %v", rollbackErr)
		}
		log.Printf("Ошибка выполнения PL/SQL для pcsystem.PKG_SMS.GET_TEST_PHONE(): %v", err)
		return "", fmt.Errorf("ошибка выполнения PL/SQL: %w", err)
	}

	// Получаем результат через функцию-геттер пакета (аналогично queue_reader.go)
	query := "SELECT temp_test_phone_pkg.get_phone_number() FROM DUAL"
	var testPhone sql.NullString
	err = tx.QueryRowContext(queryCtx, query).Scan(&testPhone)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("Ошибка отката транзакции: %v", rollbackErr)
		}
		log.Printf("Ошибка выполнения SELECT для temp_test_phone_pkg.get_phone_number(): %v", err)
		return "", fmt.Errorf("ошибка получения тестового номера: %w", err)
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	if !testPhone.Valid || testPhone.String == "" {
		return "", fmt.Errorf("ошибка получения тестового номера: номер пуст")
	}

	log.Printf("pcsystem.PKG_SMS.GET_TEST_PHONE() result: L_PHONE_NUMBER = %s", testPhone.String)
	return testPhone.String, nil
}

// ensureTestPhonePackageExists создает временный пакет Oracle для работы с функцией GET_TEST_PHONE
// Аналогично queue_reader.go - Oracle требует использования пакетных переменных для передачи данных
func (d *DBConnection) ensureTestPhonePackageExists(ctx context.Context) error {
	// Создаем пакет с переменной и функцией-геттером для доступа к результату
	createPackageSQL := `
		CREATE OR REPLACE PACKAGE temp_test_phone_pkg AS
			g_phone_number VARCHAR2(500);
			
			FUNCTION get_phone_number RETURN VARCHAR2;
		END temp_test_phone_pkg;
	`
	_, err := d.db.ExecContext(ctx, createPackageSQL)
	if err != nil {
		return fmt.Errorf("не удалось создать пакет temp_test_phone_pkg: %w", err)
	}

	// Создаем тело пакета с реализацией функции-геттера
	createPackageBodySQL := `
		CREATE OR REPLACE PACKAGE BODY temp_test_phone_pkg AS
			FUNCTION get_phone_number RETURN VARCHAR2 IS
			BEGIN
				RETURN g_phone_number;
			END;
		END temp_test_phone_pkg;
	`
	_, err = d.db.ExecContext(ctx, createPackageBodySQL)
	if err != nil {
		return fmt.Errorf("не удалось создать тело пакета temp_test_phone_pkg: %w", err)
	}

	return nil
}
