package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/godror/godror"
	"go.uber.org/zap"
	"gopkg.in/ini.v1"

	"oracle-client/logger"
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

// NewDBConnection загружает конфигурацию из settings/settings.ini.
// Ожидаются секция [main] и ключи: username, password, dsn, username_write, password_write.
func NewDBConnection() (*DBConnection, error) {
	settingsPath := "./settings/settings.ini"
	if _, err := os.Stat(settingsPath); os.IsNotExist(err) {
		// Используем стандартный вывод до инициализации логгера
		os.Stderr.WriteString(fmt.Sprintf("файл настроек не найден: %s\n", settingsPath))
		return nil, fmt.Errorf("файл настроек не найден: %s", settingsPath)
	}

	cfg, err := ini.Load(settingsPath)
	if err != nil {
		// Используем стандартный вывод до инициализации логгера
		os.Stderr.WriteString(fmt.Sprintf("не удалось прочитать файл настроек %s: %v\n", settingsPath, err))
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
	if logger.Log != nil {
		logger.Log.Info("Database connection closed")
	}
}

// CheckConnection проверяет соединение с БД (аналогично Python: connection.ping())
func (d *DBConnection) CheckConnection() bool {
	if d.db == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := d.db.PingContext(ctx); err != nil {
		if logger.Log != nil {
			logger.Log.Debug("Ошибка проверки соединения", zap.Error(err))
		}
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
			if logger.Log != nil {
				logger.Log.Debug("Обнаружены активные операции, ожидание",
					zap.Int32("activeOps", activeCount),
					zap.Int("retry", retry+1),
					zap.Int("maxRetries", maxRetries))
			}
			time.Sleep(retryDelay)
		} else {
			// Последняя попытка - ждем завершения операций
			if logger.Log != nil {
				logger.Log.Debug("Последняя попытка: ожидание завершения активных операций",
					zap.Int32("activeOps", activeCount))
			}
			if err := d.waitForActiveOperations(); err != nil {
				return fmt.Errorf("не удалось дождаться завершения активных операций: %w", err)
			}
			// Финальная проверка
			activeCount = d.activeOps.Load()
			if activeCount > 0 && logger.Log != nil {
				logger.Log.Warn("Переподключение выполняется при наличии активных операций",
					zap.Int32("activeOps", activeCount))
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
	if finalActiveCount > 0 && logger.Log != nil {
		logger.Log.Warn("Обнаружены активные операции под блокировкой, продолжаем переподключение",
			zap.Int32("activeOps", finalActiveCount))
	}

	if logger.Log != nil {
		logger.Log.Info("Начало переподключения к базе данных...")
	}

	// Закрываем текущий пул соединений
	if d.db != nil {
		_ = d.db.Close()
		d.db = nil
		if logger.Log != nil {
			logger.Log.Debug("Текущий пул соединений закрыт")
		}
	}

	// Пересоздаем подключение (открываем новый пул)
	if err := d.openConnectionInternal(); err != nil {
		return fmt.Errorf("ошибка переподключения: %w", err)
	}

	if logger.Log != nil {
		logger.Log.Info("Переподключение к базе данных выполнено успешно. Пул соединений пересоздан")
	}
	return nil
}

// waitForActiveOperations ждет завершения активных операций перед переподключением
// Максимальное время ожидания - 35 секунд
func (d *DBConnection) waitForActiveOperations() error {
	const maxWaitTime = 35 * time.Second
	const checkInterval = 100 * time.Millisecond
	const logInterval = 1 * time.Second // Логируем не чаще раза в секунду

	startTime := time.Now()
	lastLogTime := time.Time{}
	lastActiveCount := int32(-1)

	for {
		activeCount := d.activeOps.Load()
		if activeCount == 0 {
			return nil // Нет активных операций, можно переподключаться
		}

		// Проверяем, не превышен ли максимальный срок ожидания
		if time.Since(startTime) > maxWaitTime {
			if logger.Log != nil {
				logger.Log.Warn("Превышено время ожидания завершения активных операций",
					zap.Int32("activeOps", activeCount))
			}
			// Продолжаем переподключение, но логируем предупреждение
			return nil
		}

		// Логируем только если прошло более 1 секунды с последнего логирования
		// или изменилось количество активных операций
		now := time.Now()
		if now.Sub(lastLogTime) >= logInterval || activeCount != lastActiveCount {
			if logger.Log != nil {
				logger.Log.Debug("Ожидание завершения активных операций перед переподключением",
					zap.Int32("activeOps", activeCount))
			}
			lastLogTime = now
			lastActiveCount = activeCount
		}

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
		if logger.Log != nil {
			logger.Log.Error("Ошибка sql.Open", zap.Error(err))
		}
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
		if logger.Log != nil {
			logger.Log.Error("Ошибка ping", zap.Error(err))
		}
		return fmt.Errorf("ошибка ping: %w", err)
	}
	d.db = db
	d.lastReconnect = time.Now()
	if logger.Log != nil {
		logger.Log.Info("Database connection opened (using Oracle Instant Client via godror)")
	}
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

		if logger.Log != nil {
			logger.Log.Info("Запущен механизм периодического переподключения к БД",
				zap.Duration("interval", d.reconnectInterval))
		}

		for {
			select {
			case <-d.reconnectTicker.C:
				// Время переподключения
				if logger.Log != nil {
					logger.Log.Info("Периодическое переподключение к БД",
						zap.Duration("sinceLastReconnect", time.Since(d.lastReconnect)))
				}
				if err := d.Reconnect(); err != nil {
					if logger.Log != nil {
						logger.Log.Error("Ошибка периодического переподключения", zap.Error(err))
					}
					// Продолжаем работу, попробуем в следующий раз
				}

			case <-d.reconnectStop:
				if logger.Log != nil {
					logger.Log.Info("Остановка механизма периодического переподключения к БД")
				}
				return

			case <-d.ctx.Done():
				if logger.Log != nil {
					logger.Log.Info("Контекст отменен, остановка механизма периодического переподключения к БД")
				}
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
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения PL/SQL для pcsystem.PKG_SMS.GET_TEST_PHONE()", zap.Error(err))
		}
		return "", fmt.Errorf("ошибка выполнения PL/SQL: %w", err)
	}

	// Получаем результат через функцию-геттер пакета (аналогично queue_reader.go)
	query := "SELECT temp_test_phone_pkg.get_phone_number() FROM DUAL"
	var testPhone sql.NullString
	err = tx.QueryRowContext(queryCtx, query).Scan(&testPhone)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения SELECT для temp_test_phone_pkg.get_phone_number()", zap.Error(err))
		}
		return "", fmt.Errorf("ошибка получения тестового номера: %w", err)
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	// Если тестовый номер пуст (например, значение отсутствует в БД),
	// считаем, что отправлять SMS некуда и просто фиксируем событие в логах.
	if !testPhone.Valid || testPhone.String == "" {
		errText := "Режим Debug: тестовый номер отсутствует, SMS не отправляется"
		if logger.Log != nil {
			logger.Log.Warn(errText)
		}
		return "", fmt.Errorf("ошибка получения тестового номера: номер пуст")
	}

	if logger.Log != nil {
		logger.Log.Debug("pcsystem.PKG_SMS.GET_TEST_PHONE() result",
			zap.String("phoneNumber", testPhone.String))
	}
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
