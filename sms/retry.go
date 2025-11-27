package sms

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

var (
	// randSource используется для генерации jitter в повторных попытках
	// Инициализируется один раз при первом использовании
	randSource     *rand.Rand
	randSourceOnce sync.Once
	randSourceMu   sync.Mutex // Мьютекс для защиты доступа к randSource
)

// initRandSource инициализирует генератор случайных чисел для jitter
func initRandSource() {
	randSourceOnce.Do(func() {
		randSource = rand.New(rand.NewSource(time.Now().UnixNano()))
	})
}

// RetryMessage представляет сообщение для повторной отправки
type RetryMessage struct {
	Message     SMSMessage
	RetryCount  int       // Текущее количество попыток
	FirstError  error     // Первая ошибка, которая привела к повторной попытке
	CreatedAt   time.Time // Время создания сообщения для повторной попытки
	NextRetryAt time.Time // Время следующей попытки
}

// isSMPPProviderError проверяет, является ли ошибка ошибкой SMPP провайдера
// (ошибки соединения или отправки через SMPP, а не ошибки конфигурации или валидации)
// Использует isConnectionError, которая уже проверяет все типы ошибок соединения
func (s *Service) isSMPPProviderError(err error) bool {
	if err == nil {
		return false
	}

	// Используем существующую функцию isConnectionError для проверки ошибок соединения
	// Она уже проверяет сетевые ошибки через errors.As, ошибки библиотеки go-smpp через errors.Is
	// и строковые паттерны (включая русскоязычные)
	if isConnectionError(err) {
		return true
	}

	// Проверяем дополнительные русскоязычные ошибки отправки SMS
	// (для случаев, которые не являются ошибками соединения, но связаны с SMPP)
	errStr := strings.ToLower(err.Error())
	smppErrorKeywords := []string{
		"ошибка отправки sms",
		"smpp",
	}

	for _, keyword := range smppErrorKeywords {
		if strings.Contains(errStr, keyword) {
			return true
		}
	}

	return false
}

// enqueueForRetry добавляет сообщение в очередь повторных попыток
func (s *Service) enqueueForRetry(msg SMSMessage, err error) {
	retryMsg := &RetryMessage{
		Message:     msg,
		RetryCount:  0,
		FirstError:  err,
		CreatedAt:   time.Now(),
		NextRetryAt: time.Now(), // Первая попытка сразу
	}

	s.retryQueueMu.Lock()
	s.retryQueue = append(s.retryQueue, retryMsg)
	queueLen := len(s.retryQueue)
	s.retryQueueMu.Unlock()

	if logger.Log != nil {
		logger.Log.Debug("Сообщение добавлено в очередь повторных попыток",
			zap.Int64("taskID", msg.TaskID),
			zap.Int("queueLen", queueLen))
	}
}

// calculateNextRetryDelay вычисляет задержку до следующей попытки
// Стратегия:
// - Попытки 1-5: фиксированные задержки (0, 5, 10, 15, 20 сек)
// - Попытки 6-10: экспоненциальная задержка в течение 4 часов с jitter
func calculateNextRetryDelay(retryCount int, createdAt time.Time) (time.Duration, bool) {
	const maxRetries = 10
	const maxDuration = 4 * time.Hour

	if retryCount >= maxRetries {
		return 0, false // Превышен лимит попыток
	}

	// Проверяем, не превышено ли максимальное время с момента создания
	timeSinceCreation := time.Since(createdAt)
	if timeSinceCreation >= maxDuration {
		return 0, false // Превышено максимальное время
	}

	var delay time.Duration

	if retryCount < 5 {
		// Первые 5 попыток: фиксированные задержки
		delays := []time.Duration{0, 5 * time.Second, 10 * time.Second, 15 * time.Second, 20 * time.Second}
		delay = delays[retryCount]
	} else {
		// Попытки 6-10: экспоненциальная задержка в течение оставшихся 4 часов
		remainingTime := maxDuration - timeSinceCreation

		// Экспоненциальная задержка: базовая задержка * 2^(retryCount - 5)
		baseDelay := 1 * time.Minute
		exponentialDelay := baseDelay * time.Duration(1<<uint(retryCount-5))

		// Ограничиваем максимальной задержкой в 30 минут
		if exponentialDelay > 30*time.Minute {
			exponentialDelay = 30 * time.Minute
		}

		// Не превышаем оставшееся время
		if exponentialDelay > remainingTime {
			exponentialDelay = remainingTime
		}

		delay = exponentialDelay

		// Добавляем jitter (±20%)
		initRandSource() // Инициализируем генератор случайных чисел
		jitterPercent := 0.2
		jitter := time.Duration(float64(delay) * jitterPercent)
		// Генерируем случайное значение от -jitter до +jitter (с защитой от гонок)
		randSourceMu.Lock()
		jitterValue := time.Duration(randSource.Int63n(int64(jitter*2))) - jitter
		randSourceMu.Unlock()
		delay = delay + jitterValue

		// Убеждаемся, что задержка не отрицательная
		if delay < 0 {
			delay = 0
		}
	}

	return delay, true
}

// StartRetryWorker запускает горутину для обработки повторных попыток отправки SMS
func (s *Service) StartRetryWorker() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Проверяем, не запущен ли уже worker
	if s.retryStarted {
		return
	}
	s.retryStarted = true

	s.retryWg.Add(1)
	if logger.Log != nil {
		logger.Log.Info("Запущен механизм повторных попыток отправки SMS")
	}

	go func() {
		defer s.retryWg.Done()

		// Используем тикер для периодической проверки сообщений, готовых к повторной попытке
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		// Хранилище сообщений, ожидающих повторной попытки
		pendingRetries := make([]*RetryMessage, 0)

		for {
			select {
			case <-s.retryStop:
				if logger.Log != nil {
					logger.Log.Info("Остановка механизма повторных попыток отправки SMS")
				}
				return

			case <-ticker.C:
				// Забираем новые сообщения из очереди (swap для эффективности)
				s.retryQueueMu.Lock()
				newMessages := s.retryQueue
				// Оптимизация: создаем новый слайс только если были сообщения
				if len(s.retryQueue) > 0 {
					s.retryQueue = make([]*RetryMessage, 0)
				}
				s.retryQueueMu.Unlock()

				// Добавляем новые сообщения в pendingRetries
				if len(newMessages) > 0 {
					pendingRetries = append(pendingRetries, newMessages...)
					if logger.Log != nil {
						logger.Log.Debug("Загружено новых сообщений из очереди повторных попыток", zap.Int("count", len(newMessages)))
					}
				}

				// Проверяем сообщения, готовые к повторной попытке
				now := time.Now()
				remainingRetries := make([]*RetryMessage, 0, len(pendingRetries)) // Предварительно выделяем память

				for _, retryMsg := range pendingRetries {
					if now.Before(retryMsg.NextRetryAt) {
						// Еще не время для повторной попытки
						remainingRetries = append(remainingRetries, retryMsg)
						continue
					}

					retryMsg.RetryCount++
					if logger.Log != nil {
						logger.Log.Info("Повторная попытка отправки SMS",
							zap.Int64("taskID", retryMsg.Message.TaskID),
							zap.Int("retryCount", retryMsg.RetryCount),
							zap.Int("maxRetries", 10))
					}

					// Получаем конфигурацию SMPP
					s.mu.RLock()
					smppCfg, ok := s.cfg.SMPP[retryMsg.Message.SMPPID]
					s.mu.RUnlock()

					if !ok {
						// Конфигурация не найдена - обрабатываем как ошибку SMPP провайдера
						// Пробуем еще раз (до 10 попыток), как и с другими ошибками отправки
						err := fmt.Errorf("SMPP конфигурация не найдена для ID=%d", retryMsg.Message.SMPPID)
						if logger.Log != nil {
							logger.Log.Warn("Повторная попытка неудачна",
								zap.Int64("taskID", retryMsg.Message.TaskID),
								zap.Int("retryCount", retryMsg.RetryCount),
								zap.Int("maxRetries", 10),
								zap.Error(err))
						}

						// Вычисляем задержку до следующей попытки
						delay, shouldRetry := calculateNextRetryDelay(retryMsg.RetryCount, retryMsg.CreatedAt)
						if !shouldRetry {
							if logger.Log != nil {
								logger.Log.Warn("ПРЕКРАЩЕНИЕ повторных попыток для SMS: превышен лимит попыток или время",
									zap.Int64("taskID", retryMsg.Message.TaskID),
									zap.Int("retryCount", retryMsg.RetryCount),
									zap.Time("createdAt", retryMsg.CreatedAt))
							}
							continue // Теряем сообщение
						}

						retryMsg.NextRetryAt = now.Add(delay)
						if logger.Log != nil {
							logger.Log.Debug("Следующая попытка через", zap.Duration("delay", delay))
						}
						remainingRetries = append(remainingRetries, retryMsg)
						continue
					}

					// Определяем номер телефона (с учетом Debug режима)
					phoneNumber := retryMsg.Message.PhoneNumber
					if s.cfg.Mode.Debug {
						s.mu.RLock()
						getTestPhone := s.getTestPhone
						s.mu.RUnlock()

						if getTestPhone != nil {
							testNumber, err := getTestPhone()
							if err == nil && strings.TrimSpace(testNumber) != "" {
								phoneNumber = testNumber
							}
						}
					}

					// Пытаемся отправить SMS
					messageID, err := s.sendSMS(retryMsg.Message, smppCfg, phoneNumber)
					if err != nil {
						// Проверяем, является ли это ошибкой SMPP провайдера
						if s.isSMPPProviderError(err) {
							// Вычисляем задержку до следующей попытки
							delay, shouldRetry := calculateNextRetryDelay(retryMsg.RetryCount, retryMsg.CreatedAt)
							if !shouldRetry {
								if logger.Log != nil {
									logger.Log.Warn("ПРЕКРАЩЕНИЕ повторных попыток для SMS: превышен лимит попыток или время",
										zap.Int64("taskID", retryMsg.Message.TaskID),
										zap.Int("retryCount", retryMsg.RetryCount),
										zap.Time("createdAt", retryMsg.CreatedAt))
								}
								continue // Теряем сообщение
							}

							retryMsg.NextRetryAt = now.Add(delay)
							if logger.Log != nil {
								logger.Log.Warn("Повторная попытка неудачна, следующая попытка через",
									zap.Int64("taskID", retryMsg.Message.TaskID),
									zap.Int("retryCount", retryMsg.RetryCount),
									zap.Int("maxRetries", 10),
									zap.Error(err),
									zap.Duration("nextRetryDelay", delay))
							}
							remainingRetries = append(remainingRetries, retryMsg)
						} else {
							// Это не ошибка SMPP провайдера - прекращаем повторные попытки
							if logger.Log != nil {
								logger.Log.Warn("ПРЕКРАЩЕНИЕ повторных попыток для SMS: ошибка не связана с SMPP провайдером",
									zap.Int64("taskID", retryMsg.Message.TaskID),
									zap.Error(err))
							}
						}
					} else {
						// Успешная отправка
						if logger.Log != nil {
							logger.Log.Info("ПОВТОРНАЯ ОТПРАВКА УСПЕШНА",
								zap.Int64("taskID", retryMsg.Message.TaskID),
								zap.Int("retryCount", retryMsg.RetryCount),
								zap.Int("maxRetries", 10),
								zap.String("messageID", messageID))
						}
						// Не добавляем в remainingRetries - сообщение успешно отправлено
					}
				}

				// Оптимизация памяти: если размер уменьшился значительно, пересоздаем слайс
				if len(remainingRetries) < len(pendingRetries)/2 && len(remainingRetries) > 0 {
					newSlice := make([]*RetryMessage, len(remainingRetries))
					copy(newSlice, remainingRetries)
					pendingRetries = newSlice
				} else {
					pendingRetries = remainingRetries
				}
			}
		}
	}()
}

// StopRetryWorker останавливает механизм повторных попыток
func (s *Service) StopRetryWorker() {
	s.mu.Lock()

	if !s.retryStarted {
		s.mu.Unlock()
		return
	}

	// Проверяем, не закрыт ли уже канал
	select {
	case <-s.retryStop:
		// Уже остановлен
		s.mu.Unlock()
		return
	default:
		// Закрываем канал для сигнала остановки
		close(s.retryStop)
		s.retryStarted = false
		s.mu.Unlock()

		// Ждем завершения горутины (без блокировки основного мьютекса)
		s.retryWg.Wait()

		// Создаем новый канал для следующего запуска
		s.mu.Lock()
		s.retryStop = make(chan struct{})
		s.mu.Unlock()
	}
}
