# Реализация статуса 4 "Доставлено абоненту" для Golang проекта

## Обзор

Данный документ описывает реализацию обработки delivery receipts от SMPP-провайдера для статуса 4 "Доставлено абоненту". В текущем проекте реализованы только статусы 2 (успешно отправлено) и 3 (ошибка отправки). Статус 4 используется для уведомлений о фактической доставке SMS до абонента.

## Коды статусов

| Код | Значение | Описание | Когда используется |
|-----|----------|----------|-------------------|
| `2` | Успешно отправлено | SMS успешно отправлено через SMPP | После успешного вызова `smpp.SendSms()`, до получения delivery receipt |
| `3` | Ошибка отправки/доставки | Ошибка при отправке или доставке | При ошибке отправки, отсутствии SMPP-провайдера, нарушении расписания, или при получении delivery receipt с неуспешным статусом |
| `4` | Доставлено абоненту | SMS успешно доставлено до абонента | При получении delivery receipt с `MessageState == Delivered` |

## Что такое Delivery Receipt

Delivery Receipt (DLR) - это уведомление от SMPP-провайдера о статусе доставки SMS до абонента. Оно приходит асинхронно после отправки SMS в виде PDU типа `DELIVER_SM`.

### Когда приходит Delivery Receipt

Delivery Receipt приходит только если при отправке SMS был установлен флаг `RegisteredDelivery = OnSuccessOrFailure` (или аналогичный в вашей SMPP библиотеке).

## Логика обработки Delivery Receipt

### 1. Настройка запроса Delivery Receipt при отправке SMS

При отправке SMS необходимо установить флаг запроса delivery receipt:

```go
// Пример для библиотеки github.com/fiorix/go-smpp
submitSM := &smpp.SubmitSM{
    SourceAddr:      senderName,
    DestAddr:        phoneNumber,
    ShortMessage:    messageText,
    RegisteredDelivery: smpp.RegisteredDelivery(0x01), // Запрос delivery receipt
    // или
    // RegisteredDelivery: smpp.RegisteredDelivery(smpp.FinalDeliveryReceipt | smpp.DeliveryReceiptFailure),
}
```

### 2. Обработка входящего DELIVER_SM PDU

Когда SMPP-провайдер отправляет delivery receipt, он приходит в виде PDU `DELIVER_SM`. Необходимо:

1. **Отправить ответ провайдеру** (`DELIVER_SM_RESP`) - подтверждение получения delivery receipt
2. **Извлечь данные** из delivery receipt:
   - `ReceiptedMessageId` - ID сообщения, для которого пришел receipt
   - `MessageState` - статус доставки
   - `SequenceNumber` - номер последовательности PDU
3. **Обработать статус** и сохранить в БД

### 3. Проверка MessageState

В SMPP протоколе `MessageState` может иметь следующие значения (согласно спецификации SMPP 3.4):

- `ENROUTE` (1) - сообщение в пути
- `DELIVERED` (2) - доставлено абоненту
- `EXPIRED` (3) - истек срок доставки
- `DELETED` (4) - удалено
- `UNDELIVERABLE` (5) - не доставлено
- `ACCEPTED` (6) - принято
- `UNKNOWN` (7) - неизвестно
- `REJECTED` (8) - отклонено

**Важно:** Статус 4 (доставлено абоненту) устанавливается только когда `MessageState == DELIVERED` (значение 2 в SMPP).

## Структура данных для Delivery Receipt

```go
// DeliveryReceipt представляет delivery receipt от SMPP провайдера
type DeliveryReceipt struct {
    ReceiptedMessageID string    // ID сообщения, для которого пришел receipt
    MessageState       int       // Статус доставки (SMPP MessageState)
    SequenceNumber     uint32    // Номер последовательности PDU
    ErrorText          string    // Текст ошибки (если есть)
    ReceivedAt         time.Time // Время получения receipt
}

// FireAqEvent - структура для сохранения результата в БД
type FireAqEvent struct {
    TaskID            int64     // sms_task_id из XML (для delivery receipt = -1, что означает NULL)
    ReceiptedMessageID string   // ID сообщения от SMPP
    MessageState      int       // Код статуса (2, 3 или 4)
    SequenceNumber    uint32    // Номер последовательности (не используется в процедуре)
    ErrorText         string    // Текст ошибки
    ResponseDate      time.Time // Дата/время ответа
}
```

## Реализация обработчика Delivery Receipt

### Пример для библиотеки github.com/fiorix/go-smpp

```go
package main

import (
    "log"
    "sync"
    "time"
    
    "github.com/fiorix/go-smpp/smpp"
    "github.com/fiorix/go-smpp/smpp/pdu/pdufield"
    "github.com/fiorix/go-smpp/smpp/pdu/pdutype"
)

type DeliveryReceiptHandler struct {
    mu          sync.Mutex
    resultQueue chan FireAqEvent
}

// HandleDeliveryReceipt обрабатывает входящий delivery receipt
func (h *DeliveryReceiptHandler) HandleDeliveryReceipt(deliverSM *smpp.DeliverSM) {
    h.mu.Lock()
    defer h.mu.Unlock()
    
    // 1. Отправляем ответ провайдеру (DELIVER_SM_RESP)
    resp := deliverSM.Response()
    if err := deliverSM.Respond(resp); err != nil {
        log.Printf("Ошибка отправки DELIVER_SM_RESP: %v", err)
        return
    }
    
    // 2. Извлекаем данные из delivery receipt
    receiptedMessageID := ""
    messageState := 0
    
    // Получаем ReceiptedMessageId из optional parameters
    if receiptedMsgID := deliverSM.Fields()[pdufield.ReceiptedMessageID]; receiptedMsgID != nil {
        receiptedMessageID = receiptedMsgID.String()
    }
    
    // Получаем MessageState из optional parameters
    if msgState := deliverSM.Fields()[pdufield.MessageState]; msgState != nil {
        messageState = int(msgState.Uint8())
    }
    
    // 3. Определяем статус для сохранения в БД
    var statusID int
    var errorText string
    
    // SMPP MessageState.DELIVERED = 2
    if messageState == 2 { // DELIVERED
        statusID = 4 // "Доставлено абоненту"
        errorText = ""
        log.Printf("Delivery receipt: сообщение %s доставлено абоненту", receiptedMessageID)
    } else {
        statusID = 3 // "Не доставлено"
        errorText = "SMS не доставлено до абонента"
        log.Printf("Delivery receipt: сообщение %s не доставлено (MessageState=%d)", receiptedMessageID, messageState)
    }
    
    // 4. Формируем событие для сохранения в БД
    event := FireAqEvent{
        TaskID:            -1, // Для delivery receipt всегда -1 (NULL в БД)
        ReceiptedMessageID: receiptedMessageID,
        MessageState:      statusID,
        SequenceNumber:    deliverSM.SequenceNumber(),
        ErrorText:         errorText,
        ResponseDate:       time.Now(),
    }
    
    // 5. Добавляем в очередь для сохранения в БД
    select {
    case h.resultQueue <- event:
        log.Printf("Delivery receipt добавлен в очередь: messageID=%s, status=%d", receiptedMessageID, statusID)
    default:
        log.Printf("ОШИБКА: очередь результатов переполнена, delivery receipt потерян")
    }
}
```

### Пример для библиотеки github.com/linxGnu/gosmpp

```go
package main

import (
    "log"
    "sync"
    "time"
    
    "github.com/linxGnu/gosmpp/pdu"
)

type DeliveryReceiptHandler struct {
    mu          sync.Mutex
    resultQueue chan FireAqEvent
}

// HandleDeliveryReceipt обрабатывает входящий delivery receipt
func (h *DeliveryReceiptHandler) HandleDeliveryReceipt(deliverSM *pdu.DeliverSM) {
    h.mu.Lock()
    defer h.mu.Unlock()
    
    // 1. Отправляем ответ провайдеру (DELIVER_SM_RESP)
    resp := pdu.NewDeliverSMResp()
    resp.SequenceNumber = deliverSM.SequenceNumber
    resp.CommandStatus = pdu.ESME_ROK
    
    // Отправка ответа должна быть реализована в вашем SMPP клиенте
    // resp.Send(connection)
    
    // 2. Извлекаем данные из delivery receipt
    receiptedMessageID := ""
    messageState := 0
    
    // Получаем ReceiptedMessageId из optional parameters
    if receiptedMsgIDField := deliverSM.OptionalParameters[pdu.TagReceiptedMessageID]; receiptedMsgIDField != nil {
        receiptedMessageID = string(receiptedMsgIDField.Data())
    }
    
    // Получаем MessageState из optional parameters
    if msgStateField := deliverSM.OptionalParameters[pdu.TagMessageState]; msgStateField != nil {
        if len(msgStateField.Data()) > 0 {
            messageState = int(msgStateField.Data()[0])
        }
    }
    
    // 3. Определяем статус для сохранения в БД
    var statusID int
    var errorText string
    
    // SMPP MessageState.DELIVERED = 2
    const SMPP_MESSAGE_STATE_DELIVERED = 2
    
    if messageState == SMPP_MESSAGE_STATE_DELIVERED {
        statusID = 4 // "Доставлено абоненту"
        errorText = ""
        log.Printf("Delivery receipt: сообщение %s доставлено абоненту", receiptedMessageID)
    } else {
        statusID = 3 // "Не доставлено"
        errorText = "SMS не доставлено до абонента"
        log.Printf("Delivery receipt: сообщение %s не доставлено (MessageState=%d)", receiptedMessageID, messageState)
    }
    
    // 4. Формируем событие для сохранения в БД
    event := FireAqEvent{
        TaskID:            -1, // Для delivery receipt всегда -1 (NULL в БД)
        ReceiptedMessageID: receiptedMessageID,
        MessageState:      statusID,
        SequenceNumber:    deliverSM.SequenceNumber,
        ErrorText:         errorText,
        ResponseDate:       time.Now(),
    }
    
    // 5. Добавляем в очередь для сохранения в БД
    select {
    case h.resultQueue <- event:
        log.Printf("Delivery receipt добавлен в очередь: messageID=%s, status=%d", receiptedMessageID, statusID)
    default:
        log.Printf("ОШИБКА: очередь результатов переполнена, delivery receipt потерян")
    }
}
```

## Интеграция с существующим кодом

### 1. Регистрация обработчика в SMPP клиенте

```go
// При инициализации SMPP клиента
func (s *SMPPClient) SetupDeliveryReceiptHandler() {
    handler := &DeliveryReceiptHandler{
        resultQueue: s.resultQueue, // Общая очередь результатов
    }
    
    // Регистрация обработчика зависит от используемой библиотеки
    // Пример для github.com/fiorix/go-smpp:
    // s.session.SetHandler(pdutype.DELIVER_SM, handler.HandleDeliveryReceipt)
    
    // Пример для github.com/linxGnu/gosmpp:
    // s.session.SetDeliverSMHandler(handler.HandleDeliveryReceipt)
}
```

### 2. Обработка в основном цикле

Обработка delivery receipts должна интегрироваться в существующий цикл обработки результатов:

```go
func (s *SMSService) processResults() {
    for {
        select {
        case event := <-s.resultQueue:
            // Обработка события (статусы 2, 3 или 4)
            if err := s.saveSmsResponse(event); err != nil {
                log.Printf("Ошибка сохранения результата: %v", err)
                // Возможно, добавить retry логику
            }
        case <-s.ctx.Done():
            return
        }
    }
}
```

### 3. Сохранение в БД через процедуру save_sms_response

```go
func (s *SMSService) saveSmsResponse(event FireAqEvent) error {
    // Подготовка параметров
    var taskID interface{}
    if event.TaskID == -1 {
        taskID = nil // NULL в Oracle
    } else {
        taskID = event.TaskID
    }
    
    var errorText interface{}
    if event.ErrorText == "" {
        errorText = nil // NULL в Oracle
    } else {
        errorText = event.ErrorText
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
    
    _, err := s.db.Exec(query,
        taskID,
        event.ReceiptedMessageID,
        event.MessageState, // 2, 3 или 4
        event.ResponseDate,
        errorText,
        sql.Out{Dest: &errCode},
        sql.Out{Dest: &errDesc},
    )
    
    if err != nil {
        return fmt.Errorf("ошибка вызова save_sms_response: %w", err)
    }
    
    // Проверка результата
    if errCode.Valid && errCode.Int64 != 0 {
        errMsg := ""
        if errDesc.Valid {
            errMsg = errDesc.String
        }
        return fmt.Errorf("ошибка БД: %d - %s", errCode.Int64, errMsg)
    }
    
    log.Printf("save_sms_response успешно: taskId=%v, smsId=%s, status=%d, date=%s, error=%s",
        taskID, event.ReceiptedMessageID, event.MessageState,
        event.ResponseDate.Format(time.RFC3339), event.ErrorText)
    
    return nil
}
```

## Поток обработки Delivery Receipt

```
1. SMS отправляется с RegisteredDelivery = OnSuccessOrFailure
   ↓
2. SMPP провайдер обрабатывает отправку
   ↓
3. Когда SMS доставляется (или не доставляется) абоненту, провайдер отправляет DELIVER_SM
   ↓
4. Обработчик OnDeliverSm получает PDU
   ↓
5. Отправляется DELIVER_SM_RESP провайдеру (подтверждение получения)
   ↓
6. Извлекаются ReceiptedMessageId и MessageState
   ↓
7. Проверка MessageState:
   - Если MessageState == DELIVERED (2) → статус 4
   - Иначе → статус 3
   ↓
8. Формируется FireAqEvent с TaskID = -1
   ↓
9. Событие добавляется в очередь результатов
   ↓
10. В основном цикле вызывается save_sms_response() с соответствующим статусом
```

## Важные замечания

### 1. TaskID = -1 для Delivery Receipts

Для delivery receipts всегда используется `TaskID = -1`, что означает `NULL` в базе данных. Это связано с тем, что:
- Delivery receipt приходит асинхронно, возможно после перезапуска сервиса
- Связь с исходной задачей устанавливается через `ReceiptedMessageId`
- Процедура `save_sms_response` должна уметь находить запись по `ReceiptedMessageId`

### 2. Потокобезопасность

Обработка delivery receipts должна быть потокобезопасной, так как:
- Может приходить несколько delivery receipts одновременно
- Обработка происходит в отдельной горутине
- Необходимо синхронизировать доступ к очереди результатов

### 3. Обработка ошибок

Важно обрабатывать следующие ошибки:
- Ошибка отправки `DELIVER_SM_RESP` провайдеру
- Ошибка парсинга delivery receipt
- Переполнение очереди результатов
- Ошибка сохранения в БД (реализовать retry)

### 4. Логирование

Рекомендуется логировать:
- Получение delivery receipt (с ReceiptedMessageId и MessageState)
- Определение статуса (4 или 3)
- Успешное сохранение в БД
- Ошибки обработки

## Примеры тестирования

### Тест успешной доставки

```go
func TestDeliveryReceiptDelivered(t *testing.T) {
    handler := &DeliveryReceiptHandler{
        resultQueue: make(chan FireAqEvent, 1),
    }
    
    // Создаем mock delivery receipt с MessageState = DELIVERED (2)
    deliverSM := createMockDeliverSM("MSG-123", 2) // DELIVERED
    
    handler.HandleDeliveryReceipt(deliverSM)
    
    event := <-handler.resultQueue
    
    assert.Equal(t, -1, event.TaskID)
    assert.Equal(t, "MSG-123", event.ReceiptedMessageID)
    assert.Equal(t, 4, event.MessageState) // "Доставлено абоненту"
    assert.Empty(t, event.ErrorText)
}
```

### Тест неуспешной доставки

```go
func TestDeliveryReceiptUndelivered(t *testing.T) {
    handler := &DeliveryReceiptHandler{
        resultQueue: make(chan FireAqEvent, 1),
    }
    
    // Создаем mock delivery receipt с MessageState = UNDELIVERABLE (5)
    deliverSM := createMockDeliverSM("MSG-123", 5) // UNDELIVERABLE
    
    handler.HandleDeliveryReceipt(deliverSM)
    
    event := <-handler.resultQueue
    
    assert.Equal(t, -1, event.TaskID)
    assert.Equal(t, "MSG-123", event.ReceiptedMessageID)
    assert.Equal(t, 3, event.MessageState) // "Не доставлено"
    assert.Equal(t, "SMS не доставлено до абонента", event.ErrorText)
}
```

## Константы для MessageState

Рекомендуется определить константы для SMPP MessageState:

```go
// SMPP MessageState значения (согласно спецификации SMPP 3.4)
const (
    SMPP_MESSAGE_STATE_ENROUTE      = 1
    SMPP_MESSAGE_STATE_DELIVERED   = 2 // Используется для статуса 4
    SMPP_MESSAGE_STATE_EXPIRED      = 3
    SMPP_MESSAGE_STATE_DELETED      = 4
    SMPP_MESSAGE_STATE_UNDELIVERABLE = 5
    SMPP_MESSAGE_STATE_ACCEPTED     = 6
    SMPP_MESSAGE_STATE_UNKNOWN      = 7
    SMPP_MESSAGE_STATE_REJECTED     = 8
)

// Статусы для сохранения в БД
const (
    STATUS_SENT_SUCCESSFULLY    = 2 // Успешно отправлено
    STATUS_ERROR_OR_UNDELIVERED = 3 // Ошибка отправки или не доставлено
    STATUS_DELIVERED            = 4 // Доставлено абоненту
)
```

## Заключение

Реализация статуса 4 требует:

1. ✅ Настройки запроса delivery receipt при отправке SMS (`RegisteredDelivery`)
2. ✅ Регистрации обработчика входящих `DELIVER_SM` PDU
3. ✅ Отправки `DELIVER_SM_RESP` провайдеру
4. ✅ Извлечения `ReceiptedMessageId` и `MessageState` из delivery receipt
5. ✅ Проверки `MessageState == DELIVERED` для определения статуса 4
6. ✅ Сохранения результата в БД через `save_sms_response()` с `TaskID = -1` и статусом 4

После реализации статуса 4 система будет полностью отслеживать жизненный цикл SMS: отправка (статус 2) → доставка (статус 4) или ошибка доставки (статус 3).

