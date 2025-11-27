package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"oracle-client/logger"
)

// SaveSmsResponseParams представляет параметры для вызова процедуры save_sms_response
type SaveSmsResponseParams struct {
	TaskID       int64     // P_SMS_TASK_ID (может быть -1 для NULL)
	MessageID    string    // P_MESSAGE_ID
	StatusID     int       // P_STATUS_ID (2 - успех, 3 - ошибка)
	ResponseDate time.Time // P_DATE_RESPONSE
	ErrorText    string    // P_ERROR_TEXT (может быть пустой для NULL)
}

// SaveSmsResponse вызывает процедуру pcsystem.pkg_sms.save_sms_response() для сохранения результатов отправки SMS
// Использует блокировку для потокобезопасности
// Принимает контекст для возможности отмены операций при graceful shutdown
// Возвращает true при успешном выполнении, false при ошибке
func (d *DBConnection) SaveSmsResponse(ctx context.Context, params SaveSmsResponseParams) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db == nil {
		return false, fmt.Errorf("соединение с БД не открыто")
	}

	// Проверяем соединение
	if !d.CheckConnection() {
		return false, fmt.Errorf("соединение с БД недоступно")
	}

	// Отмечаем начало операции с БД для предотвращения переподключения во время транзакции
	d.BeginOperation()
	defer d.EndOperation()

	// Создаем контекст с таймаутом для транзакции, объединяя с переданным контекстом
	// Это позволяет отменить транзакцию при graceful shutdown
	queryCtx, queryCancel := context.WithTimeout(ctx, execTimeout)
	defer queryCancel()

	// Подготовка параметров
	var taskID interface{}
	if params.TaskID == -1 {
		taskID = nil // NULL в Oracle
	} else {
		taskID = params.TaskID
	}

	var errorText interface{}
	if params.ErrorText == "" {
		errorText = nil // NULL в Oracle
	} else {
		errorText = params.ErrorText
	}

	// Выполняем операции в транзакции для обеспечения атомарности
	tx, err := d.db.BeginTx(queryCtx, nil)
	if err != nil {
		return false, fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback() // Откатываем, если что-то пойдет не так

	// Вызов процедуры с OUT параметрами
	var errCode sql.NullInt64
	var errDesc sql.NullString

	query := `
		BEGIN
			pcsystem.pkg_sms.save_sms_response(
				P_SMS_TASK_ID => :1,
				P_MESSAGE_ID => :2,
				P_STATUS_ID => :3,
				P_DATE_RESPONSE => :4,
				P_ERROR_TEXT => :5,
				P_ERR_CODE => :6,
				P_ERR_DESC => :7
			);
		END;`

	_, err = tx.ExecContext(queryCtx, query,
		taskID,
		params.MessageID,
		params.StatusID,
		params.ResponseDate,
		errorText,
		sql.Out{Dest: &errCode},
		sql.Out{Dest: &errDesc},
	)

	if err != nil {
		// Проверяем, была ли операция отменена из-за graceful shutdown
		if ctx.Err() == context.Canceled {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
				logger.Log.Warn("Ошибка отката транзакции при отмене контекста", zap.Error(rollbackErr))
			}
			if logger.Log != nil {
				logger.Log.Warn("Сохранение результата SMS отменено из-за graceful shutdown",
					zap.Int64("taskID", params.TaskID),
					zap.String("messageID", params.MessageID))
			}
			return false, fmt.Errorf("операция отменена: %w", ctx.Err())
		}

		if rollbackErr := tx.Rollback(); rollbackErr != nil && logger.Log != nil {
			logger.Log.Error("Ошибка отката транзакции", zap.Error(rollbackErr))
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка вызова pcsystem.pkg_sms.save_sms_response",
				zap.Int64("taskID", params.TaskID),
				zap.String("messageID", params.MessageID),
				zap.Int("statusID", params.StatusID),
				zap.Error(err))
		}
		return false, fmt.Errorf("ошибка вызова save_sms_response: %w", err)
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		// Проверяем, была ли операция отменена из-за graceful shutdown
		if ctx.Err() == context.Canceled {
			if logger.Log != nil {
				logger.Log.Warn("Коммит транзакции отменен из-за graceful shutdown",
					zap.Int64("taskID", params.TaskID),
					zap.String("messageID", params.MessageID))
			}
			return false, fmt.Errorf("операция отменена: %w", ctx.Err())
		}
		return false, fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	// Проверка результата процедуры
	if errCode.Valid && errCode.Int64 != 0 {
		errMsg := ""
		if errDesc.Valid {
			errMsg = errDesc.String
		}
		if logger.Log != nil {
			logger.Log.Error("Ошибка выполнения pcsystem.pkg_sms.save_sms_response",
				zap.Int64("errCode", errCode.Int64),
				zap.String("errDesc", errMsg),
				zap.Int64("taskID", params.TaskID),
				zap.String("messageID", params.MessageID),
				zap.Int("statusID", params.StatusID))
		}
		return false, fmt.Errorf("ошибка БД: %d - %s", errCode.Int64, errMsg)
	}

	// Успешное выполнение
	if logger.Log != nil {
		logger.Log.Info("Вызов pcsystem.pkg_sms.save_sms_response() успешно",
			zap.Int64("taskID", params.TaskID),
			zap.String("messageID", params.MessageID),
			zap.Int("statusID", params.StatusID),
			zap.Time("responseDate", params.ResponseDate),
			zap.String("errorText", params.ErrorText))
	}

	return true, nil
}
