package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/ini.v1"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	// Log - глобальный экземпляр логгера
	Log *zap.Logger
)

// LogLevel представляет уровни логирования
// 0 = Panic, 1 = Fatal, 2 = Error, 3 = Warn, 4 = Info, 5 = Debug
type LogLevel int

const (
	LogLevelPanic LogLevel = iota
	LogLevelFatal
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

// InitLogger инициализирует логгер с настройками из конфигурационного файла
func InitLogger(cfg *ini.File) error {
	// Настройки по умолчанию
	logLevel := LogLevelDebug // По умолчанию debug
	maxArchiveFiles := 10
	logDir := "logs"

	// Читаем настройки из секции [Log]
	if cfg != nil && cfg.HasSection("Log") {
		logSection := cfg.Section("Log")

		// Читаем уровень логирования
		if logLevelStr := logSection.Key("LogLevel").String(); logLevelStr != "" {
			if level, err := strconv.Atoi(logLevelStr); err == nil {
				// Валидация: уровень должен быть в диапазоне от LogLevelPanic (0) до LogLevelDebug (5)
				if level >= int(LogLevelPanic) && level <= int(LogLevelDebug) {
					logLevel = LogLevel(level)
				} else {
					// Используем стандартный вывод, так как логгер еще не инициализирован
					os.Stderr.WriteString(fmt.Sprintf("Предупреждение: некорректный уровень логирования %d (допустимый диапазон: %d-%d), используется значение по умолчанию: %d\n",
						level, int(LogLevelPanic), int(LogLevelDebug), int(LogLevelDebug)))
				}
			}
		}

		// Читаем максимальное количество архивных файлов
		if maxArchiveStr := logSection.Key("MaxArchiveFiles").String(); maxArchiveStr != "" {
			if maxArchive, err := strconv.Atoi(maxArchiveStr); err == nil {
				maxArchiveFiles = maxArchive
			}
		}
	}

	// Создаем директорию для логов, если её нет
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директорию для логов: %w", err)
	}

	// Настраиваем ротацию логов с помощью lumberjack
	logFile := filepath.Join(logDir, "app.log")
	logWriter := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    100, // Максимальный размер файла в мегабайтах перед ротацией
		MaxBackups: maxArchiveFiles,
		MaxAge:     10, // Хранить логи максимум 10 дней
		Compress:   true,
		LocalTime:  true,
	}

	// Настраиваем zap core для записи в файл
	fileEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	fileCore := zapcore.NewCore(fileEncoder, zapcore.AddSync(logWriter), getZapLevel(logLevel))

	// Создаем логгер только с fileCore (логирование только в файл)
	Log = zap.New(fileCore, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	Log.Info("Логгер инициализирован",
		zap.Int("logLevel", int(logLevel)),
		zap.Int("maxArchiveFiles", maxArchiveFiles),
		zap.String("logDir", logDir))

	return nil
}

// getZapLevel преобразует LogLevel в zapcore.Level
func getZapLevel(level LogLevel) zapcore.Level {
	switch level {
	case LogLevelPanic:
		return zapcore.PanicLevel
	case LogLevelFatal:
		return zapcore.FatalLevel
	case LogLevelError:
		return zapcore.ErrorLevel
	case LogLevelWarn:
		return zapcore.WarnLevel
	case LogLevelInfo:
		return zapcore.InfoLevel
	case LogLevelDebug:
		return zapcore.DebugLevel
	default:
		return zapcore.DebugLevel // По умолчанию debug
	}
}

// Helper функции для совместимости со стандартным log пакетом

// Printf выводит сообщение с уровнем Info
func Printf(format string, args ...interface{}) {
	Log.Sugar().Infof(format, args...)
}

// Println выводит сообщение с уровнем Info
func Println(args ...interface{}) {
	Log.Sugar().Info(args...)
}

// Print выводит сообщение с уровнем Info
func Print(args ...interface{}) {
	Log.Sugar().Info(args...)
}

// Fatalf выводит сообщение с уровнем Fatal и завершает программу
func Fatalf(format string, args ...interface{}) {
	Log.Sugar().Fatalf(format, args...)
}

// Fatal выводит сообщение с уровнем Fatal и завершает программу
func Fatal(args ...interface{}) {
	Log.Sugar().Fatal(args...)
}
