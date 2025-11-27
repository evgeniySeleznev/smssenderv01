# Спецификация процедуры pcsystem.pkg_sms.save_sms_response()

## Описание

Процедура `pcsystem.pkg_sms.save_sms_response()` используется для сохранения результатов отправки SMS-сообщений в базу данных Oracle. Вызывается после каждой попытки отправки SMS через SMPP-провайдера.

## Контекст использования

Процедура вызывается в следующих случаях:
1. После успешной отправки SMS через SMPP
2. После неудачной попытки отправки SMS
3. При ошибках валидации перед отправкой (отсутствие SMPP-провайдера, нарушение расписания)

## Сигнатура процедуры

**Пакет:** `pcsystem.pkg_sms`  
**Процедура:** `save_sms_response`

### Входные параметры (Input)

| Параметр | Тип Oracle | Тип Go | Обязательный | Описание |
|----------|------------|--------|--------------|----------|
| `P_SMS_TASK_ID` | NUMBER | int64 | Да* | ID задачи SMS из таблицы `pcsystem.sms_task`. Если значение равно `-1`, передается `NULL` |
| `P_MESSAGE_ID` | VARCHAR2 | string | Да | ID сообщения, полученный от SMPP-провайдера. При ошибке отправки может быть пустой строкой |
| `P_STATUS_ID` | NUMBER | int | Да | Код статуса отправки (см. таблицу кодов статусов ниже) |
| `P_DATE_RESPONSE` | DATE | time.Time | Да | Дата и время получения ответа от SMPP-провайдера |
| `P_ERROR_TEXT` | VARCHAR2 | string | Нет | Текст ошибки. Если пустая строка, передается `NULL` |

*Примечание: Если `P_SMS_TASK_ID = -1`, параметр передается как `NULL`

### Выходные параметры (Output)

| Параметр | Тип Oracle | Тип Go | Описание |
|----------|------------|--------|----------|
| `P_ERR_CODE` | NUMBER | int | Код ошибки выполнения процедуры. `0` - успех, любое другое значение - ошибка |
| `P_ERR_DESC` | VARCHAR2 | string | Описание ошибки выполнения процедуры (если `P_ERR_CODE != 0`) |

## Коды статусов (P_STATUS_ID)

| Код | Значение | Описание | Когда используется |
|-----|----------|----------|-------------------|
| `2` | Успешно отправлено | SMS успешно отправлено через SMPP | После успешного вызова `smpp.SendSms()` |
| `3` | Ошибка отправки | Ошибка при отправке или валидации | При ошибке отправки, отсутствии SMPP-провайдера, нарушении расписания |

## Логика вызова

### 1. Подготовка параметров

```go
// Пример структуры для вызова процедуры
type SaveSmsResponseParams struct {
    TaskID      int64     // P_SMS_TASK_ID
    MessageID   string    // P_MESSAGE_ID
    StatusID    int       // P_STATUS_ID (2 - успех, 3 - ошибка)
    ResponseDate time.Time // P_DATE_RESPONSE
    ErrorText   string    // P_ERROR_TEXT (может быть пустой)
}

// Обработка специального случая taskId = -1
func prepareTaskID(taskID int64) interface{} {
    if taskID == -1 {
        return nil // NULL в Oracle
    }
    return taskID
}

// Обработка пустого errorText
func prepareErrorText(errorText string) interface{} {
    if errorText == "" {
        return nil // NULL в Oracle
    }
    return errorText
}
```

### 2. Вызов процедуры

Процедура должна вызываться с использованием транзакционной блокировки для обеспечения потокобезопасности при параллельной обработке сообщений.

**Важно:** Вызов должен быть синхронизирован (использовать мьютекс/блокировку), если несколько горутин могут вызывать процедуру одновременно.

### 3. Обработка результата

```go
type SaveSmsResponseResult struct {
    ErrCode int    // P_ERR_CODE
    ErrDesc string // P_ERR_DESC
}

// Проверка результата
if result.ErrCode != 0 {
    // Ошибка выполнения процедуры
    log.Error("Ошибка вызова save_sms_response: %d - %s", result.ErrCode, result.ErrDesc)
    return false
}

// Успешное выполнение
log.Info("save_sms_response успешно: taskId=%d, smsId=%s, status=%d, date=%s, error=%s",
    params.TaskID, params.MessageID, params.StatusID, params.ResponseDate, params.ErrorText)
return true
```

## Примеры использования

### Пример 1: Успешная отправка SMS

```go
params := SaveSmsResponseParams{
    TaskID:      12345,
    MessageID:   "SMPP-2024-01-15-001",
    StatusID:    2,
    ResponseDate: time.Now(),
    ErrorText:   "",
}

result := callSaveSmsResponse(params)
if !result {
    // Обработка ошибки
}
```

### Пример 2: Ошибка отправки (нет SMPP-провайдера)

```go
params := SaveSmsResponseParams{
    TaskID:      12345,
    MessageID:   "", // Пустой, т.к. SMS не был отправлен
    StatusID:    3,
    ResponseDate: time.Now(),
    ErrorText:   "SMS не отправлено: нет данных для указанного SMPP",
}

result := callSaveSmsResponse(params)
```

### Пример 3: Ошибка отправки (нарушение расписания)

```go
params := SaveSmsResponseParams{
    TaskID:      12345,
    MessageID:   "",
    StatusID:    3,
    ResponseDate: time.Now(),
    ErrorText:   fmt.Sprintf("Попытка отправки SMS за пределами расписания [%s..%s]", 
        timeStart.Format("15:04:05"), timeEnd.Format("15:04:05")),
}

result := callSaveSmsResponse(params)
```

### Пример 4: Ошибка отправки (ошибка SMPP)

```go
params := SaveSmsResponseParams{
    TaskID:      12345,
    MessageID:   "",
    StatusID:    3,
    ResponseDate: time.Now(),
    ErrorText:   "Ошибка отправки абоненту",
}

result := callSaveSmsResponse(params)
```

### Пример 5: Специальный случай (taskId = -1)

```go
params := SaveSmsResponseParams{
    TaskID:      -1, // Будет передано как NULL
    MessageID:   "SMPP-2024-01-15-002",
    StatusID:    2,
    ResponseDate: time.Now(),
    ErrorText:   "",
}

result := callSaveSmsResponse(params)
```

## Обработка ошибок

### Ошибки выполнения процедуры

Если процедура возвращает `P_ERR_CODE != 0`, это означает ошибку на стороне базы данных:
- Проблемы с подключением к БД
- Нарушение ограничений целостности данных
- Ошибки в самой процедуре

В этом случае необходимо:
1. Залогировать ошибку с кодом и описанием
2. Вернуть `false` из функции вызова
3. Возможно, повторить попытку вызова (с учетом retry-логики)

### Исключения при вызове

При возникновении исключений (network errors, timeout и т.д.):
1. Захватить исключение
2. Залогировать с полным контекстом
3. Вернуть `false`
4. Убедиться, что блокировка освобождена (в finally/defer)

## Потокобезопасность

**Критически важно:** Вызов процедуры должен быть защищен блокировкой (мьютексом), если:
- Несколько горутин обрабатывают сообщения параллельно
- Используется одно подключение к БД для всех вызовов

Рекомендуемый подход:
```go
var saveResponseMutex sync.Mutex

func callSaveSmsResponse(params SaveSmsResponseParams) bool {
    saveResponseMutex.Lock()
    defer saveResponseMutex.Unlock()
    
    // Проверка подключения к БД
    if !isConnected() {
        return false
    }
    
    // Вызов процедуры
    // ...
}
```

## Интеграция в общий поток обработки

Процедура вызывается в следующем контексте:

```
1. Получение сообщения из Oracle AQ очереди
   ↓
2. Парсинг XML и извлечение данных (включая sms_task_id)
   ↓
3. Валидация (проверка SMPP-провайдера, расписания)
   ↓
4. Отправка через SMPP (или ошибка валидации)
   ↓
5. Формирование результата (FireAqEvent)
   ↓
6. Добавление в очередь результатов
   ↓
7. В основном цикле: извлечение из очереди и вызов save_sms_response()
```

## Структура данных FireAqEvent

Для хранения результатов перед вызовом процедуры используется структура:

```go
type FireAqEvent struct {
    TaskID            int64     // sms_task_id из XML
    ReceiptedMessageID string   // ID сообщения от SMPP (может быть пустым)
    MessageState      int       // Код статуса (2 или 3)
    SequenceNumber    uint      // Номер последовательности (не используется в процедуре)
    ErrorText         string    // Текст ошибки
    ResponseDate      time.Time // Дата/время ответа
}
```

## Логирование

Рекомендуется логировать:

1. **Успешный вызов:**
   ```
   INFO: Вызов pcsystem.pkg_sms.save_sms_response(): taskId, smsId, statusCode, responseDate, errorText
   ```

2. **Ошибка процедуры:**
   ```
   ERROR: Вызов pcsystem.pkg_sms.save_sms_response(): errCode errDesc
   ```

3. **Исключение при вызове:**
   ```
   ERROR: Вызов pcsystem.pkg_sms.save_sms_response(): [исключение с полным стеком]
   ```

## Зависимости

- Подключение к Oracle Database
- Пакет `pcsystem.pkg_sms` должен быть доступен в БД
- Права на выполнение процедуры `save_sms_response`

## Примечания для реализации на Golang

1. Используйте библиотеку для работы с Oracle (например, `github.com/godror/godror` или `github.com/sijms/go-ora`)
2. Обрабатывайте NULL значения для `P_SMS_TASK_ID` (когда taskId = -1) и `P_ERROR_TEXT` (когда пустая строка)
3. Используйте `time.Time` для даты/времени, конвертируя в Oracle DATE
4. Обеспечьте потокобезопасность через мьютексы
5. Реализуйте retry-логику для сетевых ошибок
6. Используйте connection pooling для эффективной работы с БД

## Пример полной реализации на Golang

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    "sync"
    "time"
    
    _ "github.com/godror/godror"
)

type SaveSmsResponseParams struct {
    TaskID      int64
    MessageID   string
    StatusID    int
    ResponseDate time.Time
    ErrorText   string
}

var saveResponseMutex sync.Mutex

func SaveSmsResponse(db *sql.DB, params SaveSmsResponseParams) (bool, error) {
    saveResponseMutex.Lock()
    defer saveResponseMutex.Unlock()
    
    // Подготовка параметров
    var taskID interface{}
    if params.TaskID == -1 {
        taskID = nil
    } else {
        taskID = params.TaskID
    }
    
    var errorText interface{}
    if params.ErrorText == "" {
        errorText = nil
    } else {
        errorText = params.ErrorText
    }
    
    // Вызов процедуры
    var errCode sql.NullInt64
    var errDesc sql.NullString
    
    query := `BEGIN
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
    
    _, err := db.Exec(query,
        taskID,
        params.MessageID,
        params.StatusID,
        params.ResponseDate,
        errorText,
        sql.Out{Dest: &errCode},
        sql.Out{Dest: &errDesc},
    )
    
    if err != nil {
        log.Printf("Ошибка вызова save_sms_response: %v", err)
        return false, err
    }
    
    // Проверка результата
    if errCode.Valid && errCode.Int64 != 0 {
        errMsg := ""
        if errDesc.Valid {
            errMsg = errDesc.String
        }
        log.Printf("Ошибка выполнения save_sms_response: %d - %s", errCode.Int64, errMsg)
        return false, fmt.Errorf("ошибка БД: %d - %s", errCode.Int64, errMsg)
    }
    
    log.Printf("save_sms_response успешно: taskId=%d, smsId=%s, status=%d, date=%s, error=%s",
        params.TaskID, params.MessageID, params.StatusID, 
        params.ResponseDate.Format(time.RFC3339), params.ErrorText)
    
    return true, nil
}
```

