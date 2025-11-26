package sms

import (
	"log"
	"math/rand"
	"strings"
	"time"
)

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
func (s *Service) isSMPPProviderError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Проверяем типичные ошибки SMPP провайдера
	smppErrors := []string{
		"ошибка отправки sms",
		"ошибка соединения",
		"ошибка подключения",
		"ошибка переподключения",
		"not connected",
		"not bound",
		"connection",
		"dial tcp",
		"connectex",
		"timeout",
		"broken pipe",
		"connection reset",
		"connection refused",
		"no such host",
		"network is unreachable",
		"smpp",
	}

	for _, smppErr := range smppErrors {
		if strings.Contains(errStr, smppErr) {
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

	log.Printf("Сообщение (TaskID=%d) добавлено в очередь повторных попыток (всего в очереди: %d)", msg.TaskID, queueLen)
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
		jitterPercent := 0.2
		jitter := time.Duration(float64(delay) * jitterPercent)
		// Генерируем случайное значение от -jitter до +jitter
		jitterValue := time.Duration(rand.Int63n(int64(jitter*2))) - jitter
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
	log.Println("Запущен механизм повторных попыток отправки SMS")

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
				log.Println("Остановка механизма повторных попыток отправки SMS")
				return

			case <-ticker.C:
				// Забираем новые сообщения из очереди
				s.retryQueueMu.Lock()
				newMessages := make([]*RetryMessage, len(s.retryQueue))
				copy(newMessages, s.retryQueue)
				s.retryQueue = s.retryQueue[:0] // Очищаем очередь
				s.retryQueueMu.Unlock()

				// Добавляем новые сообщения в pendingRetries
				if len(newMessages) > 0 {
					pendingRetries = append(pendingRetries, newMessages...)
					log.Printf("Загружено %d новых сообщений из очереди повторных попыток", len(newMessages))
				}

				// Проверяем сообщения, готовые к повторной попытке
				now := time.Now()
				remainingRetries := make([]*RetryMessage, 0)

				for _, retryMsg := range pendingRetries {
					if now.Before(retryMsg.NextRetryAt) {
						// Еще не время для повторной попытки
						remainingRetries = append(remainingRetries, retryMsg)
						continue
					}

					retryMsg.RetryCount++
					log.Printf("Повторная попытка отправки SMS (TaskID=%d, попытка %d/%d)",
						retryMsg.Message.TaskID, retryMsg.RetryCount, 10)

					// Получаем конфигурацию SMPP
					s.mu.RLock()
					smppCfg, ok := s.cfg.SMPP[retryMsg.Message.SMPPID]
					s.mu.RUnlock()

					if !ok {
						log.Printf("ОШИБКА: SMPP конфигурация не найдена для ID=%d (TaskID=%d), прекращаем повторные попытки",
							retryMsg.Message.SMPPID, retryMsg.Message.TaskID)
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
								log.Printf("ПРЕКРАЩЕНИЕ повторных попыток для SMS (TaskID=%d): превышен лимит попыток (%d) или время (создано: %v)",
									retryMsg.Message.TaskID, retryMsg.RetryCount, retryMsg.CreatedAt)
								continue // Теряем сообщение
							}

							retryMsg.NextRetryAt = now.Add(delay)
							log.Printf("Повторная попытка неудачна (TaskID=%d, попытка %d/%d): %v. Следующая попытка через %v",
								retryMsg.Message.TaskID, retryMsg.RetryCount, 10, err, delay)
							remainingRetries = append(remainingRetries, retryMsg)
						} else {
							// Это не ошибка SMPP провайдера - прекращаем повторные попытки
							log.Printf("ПРЕКРАЩЕНИЕ повторных попыток для SMS (TaskID=%d): ошибка не связана с SMPP провайдером: %v",
								retryMsg.Message.TaskID, err)
						}
					} else {
						// Успешная отправка
						log.Printf("ПОВТОРНАЯ ОТПРАВКА УСПЕШНА (TaskID=%d, попытка %d/%d): MessageID=%s",
							retryMsg.Message.TaskID, retryMsg.RetryCount, 10, messageID)
						// Не добавляем в remainingRetries - сообщение успешно отправлено
					}
				}

				pendingRetries = remainingRetries
			}
		}
	}()
}

// StopRetryWorker останавливает механизм повторных попыток
func (s *Service) StopRetryWorker() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.retryStarted {
		return
	}

	select {
	case <-s.retryStop:
		// Уже остановлен
		return
	default:
		close(s.retryStop)
		s.retryStarted = false
		s.mu.Unlock()
		s.retryWg.Wait()
		s.mu.Lock()
		// Создаем новый канал для следующего запуска
		s.retryStop = make(chan struct{})
	}
}
