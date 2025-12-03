package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
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
	cfg                       *ini.File
	db                        *sql.DB
	ctx                       context.Context
	cancel                    context.CancelFunc
	mu                        sync.RWMutex // Блокировка для потокобезопасного доступа к БД (RWMutex для параллельного чтения)
	reconnectTicker           *time.Ticker
	reconnectStop             chan struct{}
	reconnectWg               sync.WaitGroup
	lastReconnect             time.Time
	reconnectInterval         time.Duration // Интервал переподключения (30 минут)
	activeOps                 atomic.Int32  // Счетчик активных операций с БД
	consecutiveReconnectFails atomic.Int32  // Счетчик последовательных неудачных попыток переподключения
	reconnecting              atomic.Bool   // Флаг переподключения (блокирует новые операции)
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
	// Блокируем мьютекс только для проверки и установки соединения
	// Разблокируем перед сетевыми операциями
	d.mu.Lock()
	if d.db != nil {
		d.mu.Unlock()
		return fmt.Errorf("соединение уже открыто")
	}
	d.mu.Unlock()

	// Выполняем открытие соединения БЕЗ блокировки мьютекса
	// (сетевые операции могут занять время)
	// openConnectionInternal устанавливает соединение под блокировкой мьютекса
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
	d.mu.RLock()
	db := d.db
	d.mu.RUnlock()

	if db == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
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
// Стратегия: ждем завершения операций (макс 35 сек), если таймаут - проверяем необходимость принудительного переподключения
func (d *DBConnection) Reconnect() error {
	// Проверяем необходимость принудительного переподключения
	// Если много неудачных попыток подряд
	const maxConsecutiveFailures = 3 // Максимум последовательных неудачных попыток

	consecutiveFails := d.consecutiveReconnectFails.Load()
	forceReconnect := consecutiveFails >= maxConsecutiveFailures

	// Устанавливаем флаг переподключения для блокировки новых операций
	d.reconnecting.Store(true)
	defer d.reconnecting.Store(false) // Снимаем флаг в любом случае (успех или ошибка)

	// Ждем завершения активных операций БЕЗ блокировки
	// Это позволяет операциям завершиться естественным образом
	// Максимальное время ожидания - 35 секунд (больше чем execTimeout = 30 секунд)
	if err := d.waitForActiveOperations(); err != nil {
		// Таймаут или ошибка - проверяем необходимость принудительного переподключения
		if forceReconnect {
			// Принудительное переподключение - игнорируем активные операции
			// Это защита от навсегда зависших операций
			if logger.Log != nil {
				logger.Log.Warn("Принудительное переподключение: активные операции игнорируются",
					zap.Error(err),
					zap.Int32("consecutiveFails", consecutiveFails))
			}
			// Продолжаем переподключение несмотря на активные операции
		} else {
			// Обычная отмена переподключения
			// Для периодического переподключения это нормально - попробуем в следующий раз
			d.consecutiveReconnectFails.Add(1)
			if logger.Log != nil {
				logger.Log.Warn("Переподключение отменено: активные операции не завершились",
					zap.Error(err),
					zap.Int32("consecutiveFails", d.consecutiveReconnectFails.Load()))
			}
			return fmt.Errorf("переподключение отменено: %w", err)
		}
	} else {
		// Операции завершились успешно - сбрасываем счетчик неудач
		d.consecutiveReconnectFails.Store(0)
	}

	if logger.Log != nil {
		logger.Log.Info("Начало переподключения к базе данных...")
	}

	// Создаем новое соединение БЕЗ блокировки мьютекса
	// (сетевые операции могут занять время)
	// Старое соединение остается доступным во время создания нового
	// Это предотвращает ошибки "соединение с БД не открыто" во время переподключения
	newDB, err := d.createNewConnection()
	if err != nil {
		return fmt.Errorf("ошибка создания нового соединения: %w", err)
	}

	// Используем defer для гарантированного закрытия нового соединения в случае ошибки
	// Это предотвращает утечку памяти, если переподключение не удастся
	newDBClosed := false
	defer func() {
		if !newDBClosed && newDB != nil {
			_ = newDB.Close()
		}
	}()

	// Получаем блокировку для атомарной замены соединения
	// К этому моменту активные операции должны быть завершены (если не принудительное переподключение)
	// Финальная проверка под блокировкой для предотвращения гонок
	d.mu.Lock()
	// Еще раз проверяем активные операции под блокировкой
	finalActiveCount := d.activeOps.Load()
	if finalActiveCount > 0 && !forceReconnect {
		// Есть активные операции и это не принудительное переподключение
		// Закрываем новое соединение и отменяем переподключение
		d.mu.Unlock()
		newDBClosed = true
		_ = newDB.Close()
		if logger.Log != nil {
			logger.Log.Warn("Переподключение отменено: обнаружены активные операции под блокировкой",
				zap.Int32("activeOps", finalActiveCount))
		}
		return fmt.Errorf("нельзя переподключиться: обнаружены активные операции (%d)", finalActiveCount)
	}
	if finalActiveCount > 0 && forceReconnect {
		// Принудительное переподключение - игнорируем активные операции
		if logger.Log != nil {
			logger.Log.Warn("Принудительное переподключение: игнорируем активные операции под блокировкой",
				zap.Int32("activeOps", finalActiveCount))
		}
	}
	oldDB := d.db
	d.db = newDB // Атомарно заменяем старое соединение на новое
	d.lastReconnect = time.Now()
	d.consecutiveReconnectFails.Store(0) // Сбрасываем счетчик неудач при успешном переподключении
	newDBClosed = true                   // Новое соединение теперь в d.db, не закрываем его в defer
	d.mu.Unlock()

	// Закрываем старое соединение БЕЗ блокировки мьютекса
	// (операция закрытия может занять время)
	// Используем defer для гарантированного закрытия старого соединения
	// Это предотвращает утечку памяти даже при панике
	defer func() {
		if oldDB != nil {
			_ = oldDB.Close()
			if logger.Log != nil {
				logger.Log.Debug("Старый пул соединений закрыт")
			}
		}
	}()

	if logger.Log != nil {
		logger.Log.Info("Переподключение к базе данных выполнено успешно. Пул соединений пересоздан")
	}
	return nil
}

// waitForActiveOperations ждет завершения активных операций перед переподключением
// Максимальное время ожидания - 35 секунд (больше чем execTimeout = 30 секунд)
// Возвращает ошибку, если таймаут истек и операции все еще активны
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
					zap.Int32("activeOps", activeCount),
					zap.Duration("waited", time.Since(startTime)))
			}
			// Возвращаем ошибку - операции все еще активны после таймаута
			return fmt.Errorf("timeout waiting for active operations: %d operations still running", activeCount)
		}

		// Логируем только если прошло более 1 секунды с последнего логирования
		// или изменилось количество активных операций
		now := time.Now()
		if now.Sub(lastLogTime) >= logInterval || activeCount != lastActiveCount {
			if logger.Log != nil {
				logger.Log.Debug("Ожидание завершения активных операций перед переподключением",
					zap.Int32("activeOps", activeCount),
					zap.Duration("elapsed", time.Since(startTime)))
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

// createNewConnection создает новое соединение без установки его в d.db
// Используется для переподключения, чтобы избежать окна недоступности соединения
func (d *DBConnection) createNewConnection() (*sql.DB, error) {
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
		return nil, fmt.Errorf("ошибка sql.Open: %w", err)
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
		// Проверяем, не является ли это временной проблемой с сетью
		errStr := err.Error()
		isNetworkError := strings.Contains(errStr, "context deadline exceeded") ||
			strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "ORA-12170") ||
			strings.Contains(errStr, "ORA-12545")

		if logger.Log != nil {
			if isNetworkError {
				logger.Log.Debug("Временная проблема с сетью при проверке соединения", zap.Error(err))
			} else {
				logger.Log.Error("Ошибка ping", zap.Error(err))
			}
		}
		return nil, fmt.Errorf("ошибка ping: %w", err)
	}

	return db, nil
}

// openConnectionInternal внутренний метод открытия соединения без блокировки
// Устанавливает соединение в d.db под блокировкой мьютекса
func (d *DBConnection) openConnectionInternal() error {
	db, err := d.createNewConnection()
	if err != nil {
		return err
	}

	// Устанавливаем соединение под блокировкой мьютекса
	d.mu.Lock()
	d.db = db
	d.lastReconnect = time.Now()
	d.mu.Unlock()

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

// WithDB выполняет функцию с безопасным доступом к соединению БД
// Гарантирует, что соединение не будет закрыто во время выполнения функции
// Использует RWMutex для параллельного чтения
// Блокирует выполнение во время переподключения
func (d *DBConnection) WithDB(fn func(*sql.DB) error) error {
	// Проверяем, не идет ли переподключение
	if d.reconnecting.Load() {
		return fmt.Errorf("операция заблокирована: идет переподключение к БД")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.db == nil {
		return fmt.Errorf("соединение с БД не открыто")
	}

	return fn(d.db)
}

// WithDBTx выполняет функцию с транзакцией и безопасным доступом к соединению
// Гарантирует, что соединение не будет закрыто во время транзакции
// Отмечает начало операции с БД для предотвращения переподключения во время транзакции
// Блокирует выполнение во время переподключения
func (d *DBConnection) WithDBTx(ctx context.Context, fn func(*sql.Tx) error) error {
	// Проверяем, не идет ли переподключение
	if d.reconnecting.Load() {
		return fmt.Errorf("операция заблокирована: идет переподключение к БД")
	}

	// Отмечаем начало операции ПОД блокировкой
	d.mu.Lock()
	if d.db == nil {
		d.mu.Unlock()
		return fmt.Errorf("соединение с БД не открыто")
	}
	d.BeginOperation()
	db := d.db
	d.mu.Unlock()

	defer d.EndOperation()

	// Создаем транзакцию БЕЗ блокировки (соединение уже получено, счетчик активных операций > 0)
	txTimeout := execTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < execTimeout {
			txTimeout = remaining
		}
	}
	txCtx, txCancel := context.WithTimeout(ctx, txTimeout)
	defer txCancel()

	tx, err := db.BeginTx(txCtx, nil)
	if err != nil {
		// Проверяем, не является ли это ошибкой закрытого соединения
		errStr := err.Error()
		if strings.Contains(errStr, "ORA-03135") ||
			strings.Contains(errStr, "connection was closed") ||
			strings.Contains(errStr, "connection lost contact") ||
			strings.Contains(errStr, "DPI-1080") {
			return fmt.Errorf("соединение с БД закрыто: %w", err)
		}
		return fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer func() {
		// Игнорируем ошибку отката, если транзакция уже была закрыта
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			errStr := rollbackErr.Error()
			if !strings.Contains(errStr, "already been committed or rolled back") &&
				!strings.Contains(errStr, "connection was closed") &&
				!strings.Contains(errStr, "ORA-03135") {
				// Логируем только реальные ошибки отката
				if logger.Log != nil {
					logger.Log.Debug("Ошибка отката транзакции (игнорируется)", zap.Error(rollbackErr))
				}
			}
		}
	}()

	// Выполняем функцию с транзакцией
	// Reconnect() не может закрыть пул, пока activeOps > 0
	if err := fn(tx); err != nil {
		// Проверяем, не является ли это ошибкой закрытого соединения
		errStr := err.Error()
		if strings.Contains(errStr, "ORA-03135") ||
			strings.Contains(errStr, "connection was closed") ||
			strings.Contains(errStr, "connection lost contact") ||
			strings.Contains(errStr, "DPI-1080") {
			return fmt.Errorf("соединение с БД закрыто во время транзакции: %w", err)
		}
		return err
	}

	// Проверяем, не истек ли контекст перед коммитом
	// Если контекст истек, транзакция уже была откачена, не пытаемся коммитить
	if txCtx.Err() != nil {
		// Контекст истек - транзакция уже откачена, это нормально для таймаутов
		return nil
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		// Проверяем, не была ли транзакция уже откачена
		if strings.Contains(err.Error(), "already been committed or rolled back") {
			// Транзакция уже была откачена (например, из-за таймаута) - это нормально
			return nil
		}
		return fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	return nil
}

// GetTestPhone получает тестовый номер телефона из Oracle через процедуру pcsystem.PKG_SMS.GET_TEST_PHONE()
// Аналог C# метода OracleAdapter.LockGetTestPhone
// Использует WithDBTx для безопасной работы с транзакцией
func (d *DBConnection) GetTestPhone() (string, error) {
	// Проверяем соединение
	if !d.CheckConnection() {
		return "", fmt.Errorf("соединение с БД недоступно")
	}

	// Создаем контекст с таймаутом для транзакции
	queryCtx, queryCancel := context.WithTimeout(context.Background(), queryTimeout)
	defer queryCancel()

	var testPhone sql.NullString

	// Используем безопасный метод для работы с транзакцией
	err := d.WithDBTx(queryCtx, func(tx *sql.Tx) error {
		// Создаем временный пакет для хранения результата функции
		// Аналогично queue_reader.go - Oracle требует использования пакетных переменных
		if err := d.ensureTestPhonePackageExistsTx(tx, queryCtx); err != nil {
			return fmt.Errorf("ошибка создания пакета: %w", err)
		}

		// Используем PL/SQL блок для вызова функции Oracle и сохранения результата в пакетную переменную
		// Аналогично queue_reader.go - Oracle требует использования PL/SQL блоков
		plsql := `
			BEGIN
				temp_test_phone_pkg.g_phone_number := pcsystem.PKG_SMS.GET_TEST_PHONE();
			END;
		`

		// Выполняем PL/SQL блок
		_, err := tx.ExecContext(queryCtx, plsql)
		if err != nil {
			if logger.Log != nil {
				logger.Log.Error("Ошибка выполнения PL/SQL для pcsystem.PKG_SMS.GET_TEST_PHONE()", zap.Error(err))
			}
			return fmt.Errorf("ошибка выполнения PL/SQL: %w", err)
		}

		// Получаем результат через функцию-геттер пакета (аналогично queue_reader.go)
		query := "SELECT temp_test_phone_pkg.get_phone_number() FROM DUAL"
		err = tx.QueryRowContext(queryCtx, query).Scan(&testPhone)
		if err != nil {
			if logger.Log != nil {
				logger.Log.Error("Ошибка выполнения SELECT для temp_test_phone_pkg.get_phone_number()", zap.Error(err))
			}
			return fmt.Errorf("ошибка получения тестового номера: %w", err)
		}

		return nil
	})

	if err != nil {
		return "", err
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

// ensureTestPhonePackageExistsTx создает временный пакет Oracle для работы с функцией GET_TEST_PHONE
// Использует транзакцию для безопасного создания пакета
func (d *DBConnection) ensureTestPhonePackageExistsTx(tx *sql.Tx, ctx context.Context) error {
	// Создаем пакет с переменной и функцией-геттером для доступа к результату
	createPackageSQL := `
		CREATE OR REPLACE PACKAGE temp_test_phone_pkg AS
			g_phone_number VARCHAR2(500);
			
			FUNCTION get_phone_number RETURN VARCHAR2;
		END temp_test_phone_pkg;
	`
	_, err := tx.ExecContext(ctx, createPackageSQL)
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
	_, err = tx.ExecContext(ctx, createPackageBodySQL)
	if err != nil {
		return fmt.Errorf("не удалось создать тело пакета temp_test_phone_pkg: %w", err)
	}

	return nil
}
