package sms

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/ini.v1"
)

// SMPPConfig представляет конфигурацию одного SMPP провайдера
type SMPPConfig struct {
	Host                string
	Port                uint16
	User                string
	Password            string
	EnquireLinkInterval int
	DestTon             *int
	DestNpi             *int
	SourceTon           *int
	SourceNpi           *int
}

// ScheduleConfig представляет конфигурацию расписания
type ScheduleConfig struct {
	TimeStart     time.Time
	TimeEnd       time.Time
	RebindSMPPMin uint
}

// ModeConfig представляет режимы работы
type ModeConfig struct {
	Debug  bool
	Silent bool
}

// Config представляет полную конфигурацию SMS сервиса
type Config struct {
	SMPP     map[int]*SMPPConfig
	Schedule ScheduleConfig
	Mode     ModeConfig
}

// LoadConfig загружает конфигурацию из ini файла
func LoadConfig(cfg *ini.File) (*Config, error) {
	config := &Config{
		SMPP: make(map[int]*SMPPConfig),
	}

	// Парсинг секций SMPP, SMPP1, SMPP2 (для 3443 TLS порта или smpp22.smsc.ru для резерва)
	for i := 0; i < 3; i++ {
		sectionName := "SMPP"
		if i > 0 {
			sectionName = fmt.Sprintf("SMPP%d", i)
		}

		if !cfg.HasSection(sectionName) {
			continue
		}

		section := cfg.Section(sectionName)
		smppCfg := &SMPPConfig{
			Host:                section.Key("Host").String(),
			Port:                uint16(section.Key("Port").MustInt(3700)),
			User:                section.Key("User").String(),
			Password:            section.Key("Password").String(),
			EnquireLinkInterval: section.Key("EnquireLinkInterval").MustInt(0),
		}

		// Опциональные параметры
		if section.HasKey("DestTon") {
			val := section.Key("DestTon").MustInt(-1)
			if val >= 0 {
				smppCfg.DestTon = &val
			}
		}
		if section.HasKey("DestNpi") {
			val := section.Key("DestNpi").MustInt(-1)
			if val >= 0 {
				smppCfg.DestNpi = &val
			}
		}
		if section.HasKey("SourceTon") {
			val := section.Key("SourceTon").MustInt(-1)
			if val >= 0 {
				smppCfg.SourceTon = &val
			}
		}
		if section.HasKey("SourceNpi") {
			val := section.Key("SourceNpi").MustInt(-1)
			if val >= 0 {
				smppCfg.SourceNpi = &val
			}
		}

		config.SMPP[i] = smppCfg
	}

	// Парсинг секции Schedule
	if cfg.HasSection("Schedule") {
		scheduleSection := cfg.Section("Schedule")
		timeStartStr := scheduleSection.Key("TimeStart").String()
		timeEndStr := scheduleSection.Key("TimeEnd").String()

		// Парсинг времени в формате "HH:mm"
		timeStart, err := parseTime(timeStartStr)
		if err != nil {
			return nil, fmt.Errorf("invalid TimeStart format, expected HH:mm: %w", err)
		}

		timeEnd, err := parseTime(timeEndStr)
		if err != nil {
			return nil, fmt.Errorf("invalid TimeEnd format, expected HH:mm: %w", err)
		}

		// Если TimeStart > TimeEnd, меняем местами
		if timeStart.After(timeEnd) {
			timeStart, timeEnd = timeEnd, timeStart
		}

		config.Schedule = ScheduleConfig{
			TimeStart:     timeStart,
			TimeEnd:       timeEnd,
			RebindSMPPMin: uint(scheduleSection.Key("RebindSMPPMin").MustInt(0)),
		}
	}

	// Парсинг секции Mode
	if cfg.HasSection("Mode") {
		modeSection := cfg.Section("Mode")
		config.Mode = ModeConfig{
			Debug:  modeSection.Key("Debug").MustBool(false),
			Silent: modeSection.Key("Silent").MustBool(true), // По умолчанию Silent = true
		}
	} else {
		// По умолчанию Silent = true
		config.Mode = ModeConfig{
			Debug:  false,
			Silent: true,
		}
	}

	return config, nil
}

// parseTime парсит время в формате "HH:mm"
func parseTime(timeStr string) (time.Time, error) {
	parts := strings.Split(strings.TrimSpace(timeStr), ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid time format")
	}

	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, err
	}

	min, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, err
	}

	if hour < 0 || hour > 23 || min < 0 || min > 59 {
		return time.Time{}, fmt.Errorf("invalid time values")
	}

	// Используем фиксированную дату для хранения времени
	return time.Date(2000, 1, 1, hour, min, 0, 0, time.UTC), nil
}
