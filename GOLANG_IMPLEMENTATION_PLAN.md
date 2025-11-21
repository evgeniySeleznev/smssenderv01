# План реализации отправки SMS на Golang

## Анализ текущей реализации отправки SMS (C#)

### 1. Архитектура отправки SMS

Сервис использует протокол **SMPP (Short Message Peer-to-Peer) версии 3.4** для отправки SMS через провайдера.

**Основной компонент:** `SmppAdapter.cs` - класс для работы с SMPP-протоколом.

**Используемая библиотека:** `AberrantSMPP`

### 1.1 Архитектура потока данных и многопоточность

#### 1.1.1 Путь сообщения от Oracle Dequeue до отправки в SMS-сервис

**Полный путь сообщения:**

1. **Oracle AQ Queue (Advanced Queueing)**
   - Сообщения помещаются в очередь Oracle `askaq.aq_ask`
   - Очередь настроена на асинхронные уведомления (`AsyncNotification = true`)
   - Consumer: `SUB_SMS_SENDER`

2. **Асинхронное уведомление (OnMessage событие)**
   - При появлении нового сообщения Oracle генерирует событие `OnMessage`
   - Событие обрабатывается обработчиком `ListenHandler` в `SMS_Service.cs`
   - Обработчик вызывается в контексте потока Oracle-драйвера (не в основном потоке сервиса)

3. **Извлечение сообщения (Dequeue)**
   - `ListenHandler` вызывает `OracleAdapter.LockDequeueOne()`
   - Метод использует блокировку `Monitor.TryEnter` для потокобезопасности
   - Сообщение извлекается из очереди с параметрами:
     - `DequeueMode = Remove` (удаляется после получения)
     - `ConsumerName = "SUB_SMS_SENDER"`
     - `WaitTimeout = 2` секунды
   - XML-данные извлекаются из `msg.XmlPayload.GetXmlDocument()`

4. **Помещение в очередь обработки**
   - `LockDequeueOne()` вызывает `SMS_Service.EnqueueSms(xml)`
   - XML-документ помещается в `ConcurrentQueue<XmlDocument> oracleQueue`
   - Это потокобезопасная очередь, позволяющая добавлять элементы из разных потоков

5. **Основной цикл обработки (jobLoop)**
   - Выполняется в отдельном потоке, созданном в `MainLoop()`
   - В цикле проверяется наличие сообщений: `if (!oracleQueue.IsEmpty)`
   - Сообщения извлекаются последовательно: `while (oracleQueue.TryDequeue(out XmlDocument xml))`
   - Каждое сообщение обрабатывается через `HandleMessage(xml)`
   - После обработки каждого сообщения: `Thread.Sleep(20)` миллисекунд

6. **Обработка сообщения (HandleMessage)**
   - Парсинг XML-документа:
     - Извлечение `sms_task_id`, `phone_number`, `message`, `sender_name`, `smpp_id`, `sending_schedule`
     - Опциональное извлечение `date_active_from` из секции `/root/head`
   - Проверка наличия SMPP-провайдера по `smpp_id`
   - Проверка расписания (если `sending_schedule = "1"`)
   - Вызов `smpp.SendSms()` для отправки SMS

7. **Отправка через SMPP**
   - `SmppAdapter.SendSms()` создает `SmppSubmitSm` PDU
   - Отправка через SMPP-клиент (`client.Send()`)
   - Получение `smsId` от SMPP-сервера

8. **Сохранение результата**
   - Результат отправки помещается в очередь: `SMS_Service.EnqueueAnswer()`
   - Данные добавляются в `ConcurrentQueue<FireAqEvent> smppQueue`
   - В основном цикле результаты извлекаются и записываются в БД через `OracleAdapter.LockFireAqEvent()`
   - Вызывается процедура Oracle: `pcsystem.pkg_sms.save_sms_response()`

**Схема потока данных:**

```
Oracle AQ Queue
    ↓ (асинхронное событие OnMessage)
ListenHandler (поток Oracle-драйвера)
    ↓ (LockDequeueOne с блокировкой)
EnqueueSms → ConcurrentQueue<XmlDocument> oracleQueue
    ↓ (основной цикл jobLoop в отдельном потоке)
TryDequeue → HandleMessage
    ↓ (парсинг XML, валидация)
smpp.SendSms → SMPP-сервер
    ↓ (результат отправки)
EnqueueAnswer → ConcurrentQueue<FireAqEvent> smppQueue
    ↓ (основной цикл jobLoop)
LockFireAqEvent → Oracle (save_sms_response)
```

#### 1.1.2 Многопоточность

**Архитектура потоков:**

1. **Основной поток сервиса (ServiceThread)**
   - Создается в `OnStart()`: `new Thread(new ThreadStart(MainLoop))`
   - Запускает `MainLoop()`, который в свою очередь запускает `jobLoop()`
   - Обрабатывает сообщения из `oracleQueue` последовательно
   - Обрабатывает результаты из `smppQueue` последовательно
   - Выполняет периодические задачи:
     - Переподключение к SMPP (если `RebindSMPPMin > 0`)
     - Загрузка очереди из БД (если `ReloadQueueMin > 0`)
     - Проверка рабочего времени

2. **Поток Oracle-драйвера (для событий OnMessage)**
   - Вызывается Oracle-драйвером при появлении нового сообщения
   - Выполняет `ListenHandler`, который вызывает `LockDequeueOne()`
   - Использует блокировку для потокобезопасности

3. **Внутренние потоки SMPP-клиента (AberrantSMPP)**
   - Библиотека `AberrantSMPP` использует внутренние потоки для:
     - Обработки входящих PDU (delivery receipts, ответы)
     - Отправки ENQUIRE_LINK пакетов (если `EnquireLinkInterval > 0`)
     - Управления соединением
   - Обработчик `OnDeliverSm` вызывается в контексте потока SMPP-клиента

**Особенности многопоточности:**

- **Последовательная обработка сообщений**: Несмотря на использование `ConcurrentQueue`, сообщения обрабатываются **последовательно** в одном потоке (`jobLoop`). Это обеспечивает порядок обработки и упрощает логику.

- **Потокобезопасные структуры данных**:
  - `ConcurrentQueue<XmlDocument> oracleQueue` - для входящих сообщений
  - `ConcurrentQueue<FireAqEvent> smppQueue` - для результатов отправки

- **Блокировки для критических секций**:
  - `Monitor.TryEnter` в `OracleAdapter` для работы с Oracle-соединением
  - `Monitor.TryEnter` в `SmppAdapter` для работы с SMPP-клиентом
  - Блокировки предотвращают одновременный доступ к разделяемым ресурсам

- **Асинхронные операции**:
  - Oracle AQ использует асинхронные уведомления (`AsyncNotification = true`)
  - SMPP-клиент обрабатывает входящие PDU асинхронно через события

**Ограничения текущей реализации:**

- **Нет параллельной обработки**: Все сообщения обрабатываются последовательно в одном потоке
- **Блокирующие операции**: Отправка SMS через SMPP блокирует основной цикл до получения ответа
- **Задержка между сообщениями**: `Thread.Sleep(20)` между обработкой сообщений может ограничивать пропускную способность

#### 1.1.3 Вызов отправки SMS

**Точка вызова:**

Отправка SMS вызывается **синхронно** в методе `HandleMessage()`:

```319:323:SMS_Service.cs
if (smpp.SendSms(Number, SmsText, SenderName, out string smsId, smpp))
{
    EnqueueAnswer(TaskId, smsId, 2, 0, "", DateTime.Now);
    //OracleAdapter.LockFireAqEvent(TaskId, smsId, "2");
}
```

**Особенности вызова:**

- **Синхронный вызов**: `smpp.SendSms()` блокирует выполнение до получения ответа от SMPP-сервера
- **Обработка ошибок**: При ошибке отправки возвращается `false`, и результат помещается в очередь с статусом `3`
- **Повторная попытка**: Внутри `SendSms()` выполняется автоматическое переподключение при ошибке соединения
- **Результат**: После успешной отправки `smsId` помещается в очередь результатов для записи в БД

**Временная диаграмма обработки:**

```
Время →
Oracle AQ: [Сообщение появляется]
    ↓ (асинхронное событие)
ListenHandler: [LockDequeueOne] ──┐
    ↓                             │ (блокировка)
oracleQueue: [Enqueue]            │
    ↓                             │
jobLoop: [TryDequeue] ────────────┘
    ↓
HandleMessage: [Парсинг XML]
    ↓
HandleMessage: [smpp.SendSms] ────┐ (блокирующий вызов)
    ↓                              │
SMPP-сервер: [SubmitSM]            │
    ↓                              │
SMPP-сервер: [SubmitSMResp]        │
    ↓                              │
HandleMessage: [Получение smsId] ──┘
    ↓
smppQueue: [EnqueueAnswer]
    ↓
jobLoop: [LockFireAqEvent]
    ↓
Oracle: [save_sms_response]
```

### 2. Конфигурация SMPP

#### 2.1 Файл конфигурации: `sms_sender.ini`

```ini
[SMPP]
Host = smpp.smsc.ru              # Хост SMPP-сервера
Port = 3700                       # Порт SMPP-сервера
User = asklepius                  # Логин для подключения
Password = askl7194               # Пароль для подключения
EnquireLinkInterval = 0           # Интервал отправки ENQUIRE_LINK (секунды, 0 = отключено)
ReBindInterval = 0                # Интервал переподключения (секунды, 0 = отключено)
DestTon =                         # Опционально: тип номера получателя
DestNpi =                         # Опционально: план нумерации получателя
SourceTon =                       # Опционально: тип номера отправителя
SourceNpi =                       # Опционально: план нумерации отправителя
```

#### 2.2 Множественные SMPP-провайдеры

Поддерживается до **5 провайдеров**:
- `[SMPP]` - основной провайдер (ID = 0)
- `[SMPP1]` - дополнительный провайдер (ID = 1)
- `[SMPP2]` - дополнительный провайдер (ID = 2)
- `[SMPP3]` - дополнительный провайдер (ID = 3)
- `[SMPP4]` - дополнительный провайдер (ID = 4)

Каждый провайдер может иметь свои параметры подключения. Выбор провайдера происходит по полю `smpp_id` в сообщении.

#### 2.3 Параметры подключения

**Тип подключения:** `BindAsTransceiver` (двусторонняя связь для отправки и приема)

**Параметры по умолчанию:**
- **NpiType:** `National` (национальный план нумерации)
- **TonType:** `International` (международный тип номера)
- **Version:** `Version3_4` (SMPP 3.4)

**Параметры интервалов:**
- **EnquireLinkInterval:** интервал отправки пакетов поддержки соединения (0 = отключено)
- **ReBindInterval:** интервал автоматического переподключения (0 = отключено)

**Опциональные параметры адресации:**
- **DestTon/DestNpi:** тип и план нумерации для номера получателя
- **SourceTon/SourceNpi:** тип и план нумерации для номера отправителя

### 3. Процесс отправки SMS

#### 3.1 Метод отправки

**Сигнатура:** `SendSms(string number, string text, string senderName, out string smsId, SmppAdapter smpp)`

**Входные параметры:**
- `number` - номер телефона без префикса (например, `9123456789`)
- `text` - текст SMS сообщения
- `senderName` - имя отправителя (например, `ASKLEPIUS`)
- `smpp` - экземпляр SMPP-адаптера

**Выходные параметры:**
- `smsId` - ID сообщения, возвращаемый SMPP-сервером
- Возвращает `bool` - успех/неудача отправки

#### 3.2 Параметры отправляемого сообщения

```csharp
AlertOnMsgDelivery = 0x1                    // Уведомление о доставке
DataCoding = DataCoding.UCS2               // Кодировка для кириллицы
SourceAddress = senderName                  // Имя отправителя
DestinationAddress = "+7" + number         // Номер с автоматическим префиксом +7
LanguageIndicator = LanguageIndicator.Unspecified
ShortMessage = text                         // Текст сообщения
PriorityFlag = Pdu.PriorityType.Highest     // Высокий приоритет
RegisteredDelivery = OnSuccessOrFailure    // Требуется подтверждение доставки
```

**Важные детали:**
- Номер телефона автоматически дополняется префиксом `+7`
- Используется кодировка `UCS2` для поддержки кириллицы
- Приоритет установлен в `Highest`
- Требуется подтверждение доставки (`RegisteredDelivery = OnSuccessOrFailure`)
- Используется автоматическая сегментация длинных сообщений (`UseSmppSegmentation`)

#### 3.3 Обработка ошибок подключения

При ошибке отправки:
1. Проверяется состояние подключения (`client.Connected`)
2. Если не подключено, выполняется переподключение (`Bind()`)
3. Повторная попытка отправки после переподключения
4. Логирование всех ошибок

### 4. Обработка delivery receipts (подтверждений доставки)

#### 4.1 Обработчик OnDeliverSm

При получении подтверждения доставки от SMPP-сервера:

1. **Отправка ответа:**
   - Создается `DeliverSmResp` с тем же `SequenceNumber`
   - Отправляется обратно на сервер

2. **Обработка статуса доставки:**
   - Проверяется `MessageState` из `DeliverSmPdu`
   - Если `MessageState == Delivered` → статус `4` (доставлено)
   - Иначе → статус `3` (ошибка доставки)

3. **Использование ReceiptedMessageId:**
   - `ReceiptedMessageId` используется для связи с исходным сообщением
   - При обработке receipt `TaskId = -1` (связь происходит по `ReceiptedMessageId`)

#### 4.2 Статусы сообщений

- **2** - отправлено успешно (после успешного `SubmitSm`)
- **3** - ошибка отправки/доставки
- **4** - доставлено до абонента (из delivery receipt)

### 5. Управление подключением

#### 5.1 Методы подключения

**Bind()** - подключение к SMPP-серверу:
- Проверяет, не подключен ли уже клиент
- Если подключен, сначала выполняет `Unbind()`
- Выполняет подключение
- Логирует результат

**Unbind()** - отключение от SMPP-сервера:
- Проверяет состояние подключения
- Выполняет отключение
- Проверяет успешность отключения

**LockRebind(uint RebindIntervalMin)** - переподключение с блокировкой:
- Проверяет, прошло ли достаточно времени с последнего ответа
- Если да, выполняет `Unbind()` и `Bind()`
- Использует блокировку для потокобезопасности

#### 5.2 Автоматическое переподключение

**При разрыве соединения:**
- Обработчик `OnClose` срабатывает при закрытии соединения
- Если `RebindOnClose = True`, выполняется автоматическое переподключение

**Периодическое переподключение:**
- Настраивается через `RebindSMPPMin` (минуты)
- Выполняется периодически в основном цикле обработки
- Контролируется по времени последнего ответа (`LastAnswerTime`)

**Отслеживание активности:**
- `LastAnswerTime` обновляется при получении любого ответа от сервера
- Используется для определения необходимости переподключения

### 6. Тестовый контур

#### 6.1 Режим Debug

**Параметр:** `[Mode] Debug = True`

**Функционал:**
- При отправке SMS номер получателя **заменяется** на тестовый номер из БД
- Тестовый номер получается через процедуру Oracle: `pcsystem.PKG_SMS.GET_TEST_PHONE()`
- Все SMS отправляются на **один тестовый номер** независимо от исходного получателя

**Использование:**
- Тестирование без отправки реальным абонентам
- Проверка работы на реальном SMPP-провайдере
- Отладка без затрат на реальные SMS

**Код реализации:**
```csharp
if (Config.DebugMode)
{
    if (!OracleAdapter.LockGetTestPhone(out number)) return false;
}
```

#### 6.2 Режим Silent

**Параметр:** `[Mode] Silent = True`

**Функционал:**
- Полностью **отключает отправку SMS** через SMPP
- Сообщения помечаются как успешно отправленные (статус `2`)
- Позволяет тестировать работу без реальной отправки

**Код реализации:**
```csharp
if (Config.SilentMode)
{
    LogAdapter.Debug("SMS не отправляется. SilentMode = true");
    return true;  // Возвращает успех без отправки
}
```

#### 6.3 Комбинация режимов

- **Debug = True, Silent = False:** отправка на тестовый номер
- **Debug = False, Silent = True:** отправка отключена, но логируется
- **Debug = True, Silent = True:** отправка отключена, номер не заменяется

### 7. Опциональный шедулинг отправки

#### 7.1 Параметр sending_schedule

В XML-сообщении есть поле `sending_schedule`:
- **"0"** - отправка всегда разрешена (расписание не проверяется)
- **"1"** - требуется проверка расписания перед отправкой

#### 7.2 Конфигурация расписания

**Файл конфигурации:** `sms_sender.ini`

```ini
[Schedule]
TimeStart = 9:00          # Начало рабочего времени
TimeEnd = 21:00           # Конец рабочего времени
RebindSMPPMin = 60        # Интервал переподключения SMPP (минуты)
ReloadQueueMin = 60       # Интервал загрузки очереди (минуты)
RebuidQueue = False       # Переподключение к очереди при загрузке
RebindOnClose = False     # Автоматическое переподключение при закрытии
```

**Параметры:**
- `TimeStart` - время начала рабочего дня (формат: `HH:mm`)
- `TimeEnd` - время окончания рабочего дня (формат: `HH:mm`)
- Если `TimeStart > TimeEnd`, значения автоматически меняются местами

#### 7.3 Поле date_active_from

В XML-сообщении опционально может быть поле `date_active_from` в секции `/root/head`:

```xml
<root>
  <head>
    <date_active_from>2024-01-01T10:00:00</date_active_from>  <!-- опционально -->
  </head>
  <body>
    <sending_schedule>1</sending_schedule>
    ...
  </body>
</root>
```

#### 7.4 Логика проверки расписания

**Условие проверки:**
Если `sending_schedule = "1"`, то проверяется:
- `date_active_from` (если указано) должен попадать в интервал `[TimeStart, TimeEnd]` текущего дня
- Если `date_active_from` не указано, используется текущее время

**Результат проверки:**
- Если время **НЕ** попадает в интервал `[TimeStart, TimeEnd]`:
  - Отправка **отклоняется**
  - Возвращается ошибка со статусом `3`
  - Текст ошибки: `"Попытка отправки SMS за пределами расписания [TimeStart..TimeEnd]"`

**Пример:**
- `TimeStart = 9:00`, `TimeEnd = 21:00`
- `sending_schedule = "1"`, `date_active_from = "2024-01-01T08:00:00"`
- Результат: отправка отклонена (8:00 < 9:00)

- `TimeStart = 9:00`, `TimeEnd = 21:00`
- `sending_schedule = "1"`, `date_active_from = "2024-01-01T15:00:00"`
- Результат: отправка разрешена (15:00 в интервале [9:00, 21:00])

- `sending_schedule = "0"`
- Результат: отправка всегда разрешена (расписание не проверяется)

#### 7.5 Реализация в коде

```csharp
// Парсинг sending_schedule
bool SendingSchedule = (xmlDocInner.DocumentElement["sending_schedule"].InnerText == "1");

// Получение date_active_from (опционально)
DateTime dtFrom = DateTime.Now;
XmlElement activeDate = xmlDoc.DocumentElement.SelectSingleNode("/root/head/date_active_from") as XmlElement;
if (activeDate != null)
    dtFrom = DateTime.Parse(activeDate.InnerText);

// Проверка расписания
if (SendingSchedule && (dtFrom < DateTime.Now.Date.Add(Config.TimeStart) || dtFrom > DateTime.Now.Date.Add(Config.TimeEnd)))
{
    errText = String.Concat("Попытка отправки SMS за пределами расписания [", 
        DateTime.Now.Date.Add(Config.TimeStart).ToString(), "..", 
        DateTime.Now.Date.Add(Config.TimeEnd).ToString(), "]");
    EnqueueAnswer(TaskId, string.Empty, 3, 0, errText, DateTime.Now);
    return; // Отправка отклонена
}
```

### 8. Дополнительные детали реализации

#### 8.1 Формат номера телефона

- **Входящий формат:** номер без префикса (например, `9123456789`)
- **Исходящий формат:** автоматически добавляется префикс `+7` → `+79123456789`

#### 8.2 Кодировка сообщений

- Используется **UCS2** для поддержки кириллицы
- Автоматическая сегментация длинных сообщений через `UseSmppSegmentation`

#### 8.3 Приоритет и доставка

- **Приоритет:** `Highest` (высокий)
- **Подтверждение доставки:** `OnSuccessOrFailure` (требуется receipt)

#### 8.4 Потокобезопасность

- Используется `Monitor.TryEnter` для блокировок критических секций
- Блокировка при работе с SMPP-клиентом
- Блокировка при переподключении

---

## План реализации на Golang

### Важно: Текущее состояние проекта

**Уже реализовано:**
- ✅ Работа с очередью Oracle AQ (Advanced Queueing)
- ✅ Асинхронные уведомления от Oracle (`OnMessage` события)
- ✅ Извлечение сообщений из очереди (Dequeue)
- ✅ Парсинг XMLType данных из Oracle
- ✅ Структура данных с распарсенными полями из XML

**Что нужно реализовать:**
- ⏳ Интеграция с SMPP-сервером для отправки SMS
- ⏳ Обработка распарсенных данных и отправка через SMPP
- ⏳ Обработка delivery receipts (подтверждений доставки)
- ⏳ Сохранение результатов отправки обратно в Oracle
- ⏳ Управление подключением к SMPP (Bind/Unbind/Rebind)
- ⏳ Обработка ошибок и переподключение

**Входные данные (уже распарсены):**

Из Oracle очереди уже получена структура с полями:
```go
type SMSMessage struct {
    TaskID          int64      // sms_task_id
    PhoneNumber     string     // phone_number
    Message         string     // message
    SenderName      string     // sender_name
    SMPPID          int        // smpp_id (0, 1, 2, 3, 4)
    SendingSchedule bool       // sending_schedule ("0" = false, "1" = true)
    DateActiveFrom  *time.Time // date_active_from (опционально, из /root/head)
}
```

**Интеграционная точка:**

Необходимо создать функцию/метод, который принимает распарсенные данные и отправляет SMS:

```go
func SendSMS(ctx context.Context, msg SMSMessage) (string, error) {
    // msg уже содержит все распарсенные данные
    // Нужно только подключиться к SMPP и отправить
}
```

### 1. Структура проекта

```
sms-sender-go/
├── cmd/
│   └── sms-sender/
│       └── main.go              # Точка входа
├── internal/
│   ├── config/
│   │   └── config.go            # Конфигурация
│   ├── smpp/
│   │   └── adapter.go           # Адаптер SMPP
│   └── logger/
│       └── logger.go             # Логирование
├── pkg/
│   └── models/
│       └── models.go            # Модели данных
├── configs/
│   └── config.ini               # Файл конфигурации
├── go.mod
├── go.sum
└── README.md
```

### 2. Зависимости (go.mod)

```go
module sms-sender-go

go 1.21

require (
    // SMPP клиент
    github.com/fiorix/go-smpp v0.0.0-20210403173735-3484c9069e9e
    // или альтернатива: github.com/linxGnu/gosmpp v0.1.0
    
    // Парсинг INI
    gopkg.in/ini.v1 v1.67.0
    
    // Логирование
    go.uber.org/zap v1.26.0
    
    // Контекст и синхронизация
    golang.org/x/sync v0.5.0
)
```

### 3. Модели данных

**Примечание:** Структура `SMSMessage` уже реализована в проекте с распарсенными данными из Oracle. Ниже показана ожидаемая структура для интеграции.

```go
// pkg/models/models.go

package models

import "time"

// SMSMessage представляет распарсенное сообщение из Oracle очереди
// Эта структура уже реализована в проекте
type SMSMessage struct {
    TaskID          int64      // sms_task_id из XML
    PhoneNumber     string     // phone_number из XML
    Message         string     // message из XML
    SenderName      string     // sender_name из XML
    SMPPID          int        // smpp_id из XML (0, 1, 2, 3, 4)
    SendingSchedule bool       // sending_schedule из XML ("0" = false, "1" = true)
    DateActiveFrom  *time.Time // date_active_from из /root/head (опционально)
}

// SMSResponse представляет результат отправки SMS
type SMSResponse struct {
    TaskID    int64     // ID задачи из исходного сообщения
    MessageID string    // ID сообщения от SMPP-сервера
    Status    int       // Статус: 2 = отправлено, 3 = ошибка, 4 = доставлено
    ErrorText string    // Текст ошибки (если есть)
    SentAt    time.Time // Время отправки
}

// FireAqEvent представляет событие для записи в Oracle через save_sms_response
type FireAqEvent struct {
    TaskID            int64
    ReceiptedMessageID string
    MessageState      int
    SequenceNumber    uint
    ErrorText         string
    ResponseDate      time.Time
}
```

### 4. Конфигурация

```go
// internal/config/config.go

package config

import (
    "fmt"
    "strconv"
    "strings"
    "time"
    
    "gopkg.in/ini.v1"
)

type Config struct {
    SMPP     map[int]SMPPConfig
    Schedule ScheduleConfig
    Mode     ModeConfig
    Log      LogConfig
}

type ScheduleConfig struct {
    TimeStart      time.Time
    TimeEnd        time.Time
    RebindSMPPMin  uint
    ReloadQueueMin uint
    RebuildQueue   bool
    RebindOnClose  bool
}

type SMPPConfig struct {
    Host              string
    Port              uint16
    User              string
    Password          string
    EnquireLinkInterval int  // секунды, 0 = отключено
    ReBindInterval    int    // секунды, 0 = отключено
    DestTon           *int   // опционально
    DestNpi           *int   // опционально
    SourceTon         *int   // опционально
    SourceNpi         *int   // опционально
}

type ModeConfig struct {
    Debug  bool
    Silent bool
}

type LogConfig struct {
    LogLevel       int
    MaxArchiveFiles int
}

func LoadConfig(path string) (*Config, error) {
    cfg, err := ini.Load(path)
    if err != nil {
        return nil, err
    }
    
    config := &Config{
        SMPP: make(map[int]SMPPConfig),
    }
    
    // Парсинг секций SMPP, SMPP1, SMPP2, SMPP3, SMPP4
    for i := 0; i < 5; i++ {
        sectionName := "SMPP"
        if i > 0 {
            sectionName = fmt.Sprintf("SMPP%d", i)
        }
        
        if !cfg.HasSection(sectionName) {
            continue
        }
        
        section := cfg.Section(sectionName)
        smppCfg := SMPPConfig{
            Host:              section.Key("Host").String(),
            Port:              uint16(section.Key("Port").MustInt(3700)),
            User:              section.Key("User").String(),
            Password:          section.Key("Password").String(),
            EnquireLinkInterval: section.Key("EnquireLinkInterval").MustInt(0),
            ReBindInterval:    section.Key("ReBindInterval").MustInt(0),
        }
        
        // Опциональные параметры
        if section.HasKey("DestTon") {
            val := section.Key("DestTon").MustInt(-1)
            if val >= 0 {
                smppCfg.DestTon = &val
            }
        }
        // Аналогично для DestNpi, SourceTon, SourceNpi
        
        config.SMPP[i] = smppCfg
    }
    
    // Парсинг секции Schedule
    scheduleSection := cfg.Section("Schedule")
    timeStartStr := scheduleSection.Key("TimeStart").String()
    timeEndStr := scheduleSection.Key("TimeEnd").String()
    
    // Парсинг времени в формате "HH:mm"
    timeStartParts := strings.Split(timeStartStr, ":")
    if len(timeStartParts) != 2 {
        return nil, fmt.Errorf("invalid TimeStart format, expected HH:mm")
    }
    timeStartHour, _ := strconv.Atoi(timeStartParts[0])
    timeStartMin, _ := strconv.Atoi(timeStartParts[1])
    timeStart := time.Date(2000, 1, 1, timeStartHour, timeStartMin, 0, 0, time.UTC)
    
    timeEndParts := strings.Split(timeEndStr, ":")
    if len(timeEndParts) != 2 {
        return nil, fmt.Errorf("invalid TimeEnd format, expected HH:mm")
    }
    timeEndHour, _ := strconv.Atoi(timeEndParts[0])
    timeEndMin, _ := strconv.Atoi(timeEndParts[1])
    timeEnd := time.Date(2000, 1, 1, timeEndHour, timeEndMin, 0, 0, time.UTC)
    
    // Если TimeStart > TimeEnd, меняем местами
    if timeStart.After(timeEnd) {
        timeStart, timeEnd = timeEnd, timeStart
    }
    
    config.Schedule = ScheduleConfig{
        TimeStart:      timeStart,
        TimeEnd:        timeEnd,
        RebindSMPPMin:  uint(scheduleSection.Key("RebindSMPPMin").MustInt(0)),
        ReloadQueueMin: uint(scheduleSection.Key("ReloadQueueMin").MustInt(0)),
        RebuildQueue:   scheduleSection.Key("RebuildQueue").MustBool(false),
        RebindOnClose:  scheduleSection.Key("RebindOnClose").MustBool(false),
    }
    
    // Парсинг секции Mode
    modeSection := cfg.Section("Mode")
    config.Mode = ModeConfig{
        Debug:  modeSection.Key("Debug").MustBool(false),
        Silent: modeSection.Key("Silent").MustBool(false),
    }
    
    // Парсинг секции Log
    logSection := cfg.Section("Log")
    config.Log = LogConfig{
        LogLevel:       logSection.Key("LogLevel").MustInt(2),
        MaxArchiveFiles: logSection.Key("MaxArchiveFiles").MustInt(7),
    }
    
    return config, nil
}
```

### 5. SMPP Adapter

```go
// internal/smpp/adapter.go

package smpp

import (
    "context"
    "fmt"
    "sync"
    "time"
    
    "github.com/fiorix/go-smpp/smpp"
    "github.com/fiorix/go-smpp/smpp/pdu/pdubase"
)

type Adapter struct {
    client        *smpp.Transceiver
    cfg           *config.SMPPConfig
    mu            sync.Mutex
    lastAnswerTime time.Time
    onDeliveryReceipt func(receiptID string, delivered bool)
}

func NewAdapter(cfg *config.SMPPConfig) (*Adapter, error) {
    tx := &smpp.Transceiver{
        Addr:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
        User:   cfg.User,
        Passwd: cfg.Password,
    }
    
    return &Adapter{
        client: tx,
        cfg:    cfg,
        lastAnswerTime: time.Now(),
    }, nil
}

// SetDeliveryReceiptHandler устанавливает обработчик delivery receipts
func (a *Adapter) SetDeliveryReceiptHandler(handler func(receiptID string, delivered bool)) {
    a.onDeliveryReceipt = handler
}

// Bind подключается к SMPP-серверу
func (a *Adapter) Bind(ctx context.Context) error {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    if a.client.Bound() {
        a.client.Unbind()
    }
    
    if err := a.client.Bind(); err != nil {
        return fmt.Errorf("bind failed: %w", err)
    }
    
    // Настройка обработчиков
    a.client.Handler = a.handlePDU
    
    return nil
}

// Unbind отключается от SMPP-сервера
func (a *Adapter) Unbind() error {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    if !a.client.Bound() {
        return nil
    }
    
    return a.client.Unbind()
}

// SendSMS отправляет SMS
func (a *Adapter) SendSMS(ctx context.Context, number, text, senderName string) (string, error) {
    // Режим Silent - не отправляем, но возвращаем успех
    if a.cfg.Silent {
        return "silent-mode", nil
    }
    
    // Формирование номера с префиксом +7
    destinationAddr := "+7" + number
    
    // Создание SubmitSM PDU
    submit := &pdubase.SubmitSM{
        SourceAddr:      senderName,
        DestinationAddr: destinationAddr,
        ShortMessage:    []byte(text),
        DataCoding:      0x08, // UCS2
        RegisteredDelivery: 0x01, // OnSuccessOrFailure
        PriorityFlag:    0x01, // Highest
    }
    
    // Применение опциональных параметров
    if a.cfg.SourceTon != nil {
        submit.SourceAddrTon = uint8(*a.cfg.SourceTon)
    }
    if a.cfg.SourceNpi != nil {
        submit.SourceAddrNpi = uint8(*a.cfg.SourceNpi)
    }
    if a.cfg.DestTon != nil {
        submit.DestAddrTon = uint8(*a.cfg.DestTon)
    }
    if a.cfg.DestNpi != nil {
        submit.DestAddrNpi = uint8(*a.cfg.DestNpi)
    }
    
    // Отправка с автоматической сегментацией
    a.mu.Lock()
    defer a.mu.Unlock()
    
    if !a.client.Connected() {
        if err := a.Bind(ctx); err != nil {
            return "", fmt.Errorf("reconnect failed: %w", err)
        }
    }
    
    resp, err := a.client.Submit(submit)
    if err != nil {
        // Попытка переподключения и повторной отправки
        if err := a.Bind(ctx); err != nil {
            return "", fmt.Errorf("send failed and reconnect failed: %w", err)
        }
        resp, err = a.client.Submit(submit)
        if err != nil {
            return "", fmt.Errorf("send failed: %w", err)
        }
    }
    
    messageID := resp.MessageID
    a.lastAnswerTime = time.Now()
    
    return messageID, nil
}

// handlePDU обрабатывает входящие PDU
func (a *Adapter) handlePDU(p pdubase.PDU) {
    a.lastAnswerTime = time.Now()
    
    switch p := p.(type) {
    case *pdubase.DeliverSM:
        // Обработка delivery receipt
        a.handleDeliveryReceipt(p)
        
        // Отправка ответа
        resp := &pdubase.DeliverSMResp{
            Header: pdubase.Header{
                CommandID:  pdubase.DELIVER_SM_RESP,
                Sequence:   p.Header.Sequence,
            },
        }
        a.client.Send(resp)
        
    case *pdubase.EnquireLinkResp:
        // Ответ на EnquireLink - просто обновляем время
        return
        
    default:
        // Логирование неожиданных PDU
        logger.Warn("Unexpected PDU", zap.String("type", fmt.Sprintf("%T", p)))
    }
}

// handleDeliveryReceipt обрабатывает delivery receipt
func (a *Adapter) handleDeliveryReceipt(p *pdubase.DeliverSM) {
    if a.onDeliveryReceipt == nil {
        return
    }
    
    // Парсинг delivery receipt
    // В реальной реализации нужно парсить TLV и извлекать:
    // - receipted_message_id
    // - message_state (Delivered = 0x02)
    
    receiptedMessageID := extractReceiptedMessageID(p)
    messageState := extractMessageState(p)
    delivered := (messageState == 0x02) // Delivered
    
    a.onDeliveryReceipt(receiptedMessageID, delivered)
}

// Rebind выполняет переподключение
func (a *Adapter) Rebind(ctx context.Context, intervalMinutes uint) error {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    if time.Since(a.lastAnswerTime) < time.Duration(intervalMinutes)*time.Minute {
        return nil // Еще не время
    }
    
    if a.client.Bound() {
        a.client.Unbind()
    }
    
    return a.Bind(ctx)
}

// IsConnected проверяет состояние подключения
func (a *Adapter) IsConnected() bool {
    a.mu.Lock()
    defer a.mu.Unlock()
    return a.client.Connected()
}

// GetInfo возвращает информацию о подключении
func (a *Adapter) GetInfo() string {
    return fmt.Sprintf("Host=%s:%d, User=%s, Bound=%v, Connected=%v",
        a.cfg.Host, a.cfg.Port, a.cfg.User, a.client.Bound(), a.client.Connected())
}
```

### 6. Интеграция с существующей логикой Oracle

**Важно:** Работа с Oracle очередью уже реализована. Ниже показано, как интегрировать отправку SMS.

**Точка интеграции:**

В существующем коде, где обрабатываются распарсенные сообщения из Oracle, нужно добавить вызов отправки SMS:

```go
// Пример интеграции в существующем коде обработки Oracle очереди

import (
    "context"
    "sms-sender-go/internal/service"
    "sms-sender-go/pkg/models"
)

// Функция обработки сообщения из Oracle очереди
func HandleOracleMessage(ctx context.Context, msg models.SMSMessage, smsService *service.Service) error {
    // msg уже содержит все распарсенные данные:
    // - TaskID, PhoneNumber, Message, SenderName, SMPPID, SendingSchedule, DateActiveFrom
    
    // Отправка SMS через сервис
    response, err := smsService.ProcessSMS(ctx, msg)
    if err != nil {
        // Обработка ошибки
        return fmt.Errorf("failed to send SMS: %w", err)
    }
    
    // Сохранение результата обратно в Oracle
    // (логика сохранения уже должна быть реализована)
    event := models.FireAqEvent{
        TaskID:            msg.TaskID,
        ReceiptedMessageID: response.MessageID,
        MessageState:      response.Status,
        ErrorText:         response.ErrorText,
        ResponseDate:      response.SentAt,
    }
    
    // Вызов существующей функции сохранения в Oracle
    // saveToOracle(event)
    
    return nil
}
```

### 7. Основной сервис отправки SMS

```go
// internal/service/service.go

package service

import (
    "context"
    "fmt"
    "sync"
    "time"
    
    "sms-sender-go/internal/config"
    "sms-sender-go/internal/smpp"
    "sms-sender-go/pkg/models"
)

type Service struct {
    cfg          *config.Config
    smppAdapters map[int]*smpp.Adapter
    mu           sync.RWMutex
    
    // Callback для сохранения результатов в Oracle
    onResultCallback func(event models.FireAqEvent)
}

// NewService создает новый сервис отправки SMS
func NewService(cfg *config.Config) (*Service, error) {
    service := &Service{
        cfg:          cfg,
        smppAdapters: make(map[int]*smpp.Adapter),
    }
    
    // Инициализация SMPP-адаптеров
    for id, smppCfg := range cfg.SMPP {
        adapter, err := smpp.NewAdapter(&smppCfg)
        if err != nil {
            return nil, fmt.Errorf("failed to create SMPP adapter %d: %w", id, err)
        }
        
        // Настройка обработчика delivery receipts
        adapter.SetDeliveryReceiptHandler(func(receiptID string, delivered bool) {
            // Обработка delivery receipt
            // TaskID = -1, связь по ReceiptedMessageID
            status := 3 // ошибка доставки
            if delivered {
                status = 4 // доставлено
            }
            
            event := models.FireAqEvent{
                TaskID:            -1,
                ReceiptedMessageID: receiptID,
                MessageState:       status,
                ErrorText:          "",
                ResponseDate:        time.Now(),
            }
            
            if service.onResultCallback != nil {
                service.onResultCallback(event)
            }
        })
        
        service.smppAdapters[id] = adapter
    }
    
    return service, nil
}

// SetResultCallback устанавливает callback для сохранения результатов в Oracle
func (s *Service) SetResultCallback(callback func(event models.FireAqEvent)) {
    s.onResultCallback = callback
}

// Start запускает сервис
func (s *Service) Start(ctx context.Context) error {
    // Подключение ко всем SMPP-провайдерам
    for id, adapter := range s.smppAdapters {
        if err := adapter.Bind(ctx); err != nil {
            return fmt.Errorf("failed to bind SMPP adapter %d: %w", id, err)
        }
    }
    
    return nil
}

// ProcessSMS обрабатывает распарсенное сообщение из Oracle и отправляет SMS
// Это основная функция интеграции - принимает уже распарсенные данные
func (s *Service) ProcessSMS(ctx context.Context, msg models.SMSMessage) (*models.SMSResponse, error) {
    // Получение адаптера по SMPPID
    s.mu.RLock()
    adapter, ok := s.smppAdapters[msg.SMPPID]
    s.mu.RUnlock()
    
    if !ok {
        errText := fmt.Sprintf("SMS не отправлено: нет данных для указанного SMPP (ID=%d)", msg.SMPPID)
        response := &models.SMSResponse{
            TaskID:    msg.TaskID,
            MessageID: "",
            Status:    3, // ошибка
            ErrorText: errText,
            SentAt:    time.Now(),
        }
        
        // Сохранение ошибки в Oracle
        if s.onResultCallback != nil {
            s.onResultCallback(models.FireAqEvent{
                TaskID:            msg.TaskID,
                ReceiptedMessageID: "",
                MessageState:      3,
                ErrorText:         errText,
                ResponseDate:      time.Now(),
            })
        }
        
        return response, nil
    }
    
    // Проверка расписания (если требуется)
    if msg.SendingSchedule {
        if err := s.checkSchedule(msg.DateActiveFrom); err != nil {
            errText := err.Error()
            response := &models.SMSResponse{
                TaskID:    msg.TaskID,
                MessageID: "",
                Status:    3, // ошибка
                ErrorText: errText,
                SentAt:    time.Now(),
            }
            
            // Сохранение ошибки в Oracle
            if s.onResultCallback != nil {
                s.onResultCallback(models.FireAqEvent{
                    TaskID:            msg.TaskID,
                    ReceiptedMessageID: "",
                    MessageState:      3,
                    ErrorText:         errText,
                    ResponseDate:      time.Now(),
                })
            }
            
            return response, nil
        }
    }
    
    // Режим Debug - замена номера на тестовый
    phoneNumber := msg.PhoneNumber
    if s.cfg.Mode.Debug {
        // Получение тестового номера из Oracle
        // testNumber, err := oracle.GetTestPhone(ctx)
        // if err != nil {
        //     return nil, fmt.Errorf("failed to get test phone: %w", err)
        // }
        // phoneNumber = testNumber
    }
    
    // Отправка SMS
    messageID, err := adapter.SendSMS(ctx, phoneNumber, msg.Message, msg.SenderName)
    if err != nil {
        errText := fmt.Sprintf("Ошибка отправки абоненту: %v", err)
        response := &models.SMSResponse{
            TaskID:    msg.TaskID,
            MessageID: "",
            Status:    3, // ошибка
            ErrorText: errText,
            SentAt:    time.Now(),
        }
        
        // Сохранение ошибки в Oracle
        if s.onResultCallback != nil {
            s.onResultCallback(models.FireAqEvent{
                TaskID:            msg.TaskID,
                ReceiptedMessageID: "",
                MessageState:      3,
                ErrorText:         errText,
                ResponseDate:      time.Now(),
            })
        }
        
        return response, nil
    }
    
    // Успешная отправка
    response := &models.SMSResponse{
        TaskID:    msg.TaskID,
        MessageID: messageID,
        Status:    2, // отправлено успешно
        ErrorText: "",
        SentAt:    time.Now(),
    }
    
    // Сохранение успешного результата в Oracle
    if s.onResultCallback != nil {
        s.onResultCallback(models.FireAqEvent{
            TaskID:            msg.TaskID,
            ReceiptedMessageID: messageID,
            MessageState:      2,
            ErrorText:         "",
            ResponseDate:      time.Now(),
        })
    }
    
    return response, nil
}

// checkSchedule проверяет, попадает ли время в рабочее расписание
func (s *Service) checkSchedule(dateActiveFrom *time.Time) error {
    now := time.Now()
    checkTime := now
    
    // Если указана date_active_from, используем её
    if dateActiveFrom != nil {
        checkTime = *dateActiveFrom
    }
    
    // Получаем время начала и конца рабочего дня для даты проверки
    checkDate := time.Date(checkTime.Year(), checkTime.Month(), checkTime.Day(), 0, 0, 0, 0, checkTime.Location())
    
    // Извлекаем часы и минуты из TimeStart и TimeEnd
    timeStartHour := s.cfg.Schedule.TimeStart.Hour()
    timeStartMin := s.cfg.Schedule.TimeStart.Minute()
    timeEndHour := s.cfg.Schedule.TimeEnd.Hour()
    timeEndMin := s.cfg.Schedule.TimeEnd.Minute()
    
    // Формируем полное время начала и конца рабочего дня
    timeStart := checkDate.Add(time.Duration(timeStartHour)*time.Hour + time.Duration(timeStartMin)*time.Minute)
    timeEnd := checkDate.Add(time.Duration(timeEndHour)*time.Hour + time.Duration(timeEndMin)*time.Minute)
    
    // Проверка попадания в интервал
    if checkTime.Before(timeStart) || checkTime.After(timeEnd) {
        return fmt.Errorf("попытка отправки SMS за пределами расписания [%s..%s]",
            timeStart.Format("15:04"), timeEnd.Format("15:04"))
    }
    
    return nil
}

// Stop останавливает сервис
func (s *Service) Stop(ctx context.Context) error {
    for id, adapter := range s.smppAdapters {
        if err := adapter.Unbind(); err != nil {
            return fmt.Errorf("failed to unbind SMPP adapter %d: %w", id, err)
        }
    }
    return nil
}
```

**Использование сервиса в существующем коде:**

```go
// Пример полной интеграции

// 1. Инициализация сервиса
cfg, _ := config.LoadConfig("config.ini")
smsService, _ := service.NewService(cfg)

// 2. Установка callback для сохранения результатов в Oracle
smsService.SetResultCallback(func(event models.FireAqEvent) {
    // Вызов существующей функции сохранения в Oracle
    // oracle.SaveSMSResponse(event)
})

// 3. Запуск сервиса
ctx := context.Background()
smsService.Start(ctx)

// 4. В обработчике сообщений из Oracle очереди:
func onOracleMessage(msg models.SMSMessage) {
    // msg уже содержит все распарсенные данные
    response, err := smsService.ProcessSMS(ctx, msg)
    if err != nil {
        log.Error("Failed to process SMS", "error", err)
        return
    }
    
    // Результат уже сохранен через callback
    log.Info("SMS processed", "taskID", response.TaskID, "status", response.Status)
}
```

### 7.1 Сохранение результатов в Oracle

**Важно:** Сохранение результатов в Oracle должно быть реализовано в существующем коде. Ниже показан интерфейс для интеграции.

**Процедура Oracle:** `pcsystem.pkg_sms.save_sms_response`

**Параметры процедуры:**
- `P_SMS_TASK_ID` (Number, Input) - ID задачи из исходного сообщения
- `P_MESSAGE_ID` (VarChar, Input) - ID сообщения от SMPP-сервера (smsId)
- `P_STATUS_ID` (Number, Input) - Статус: 2 = отправлено, 3 = ошибка, 4 = доставлено
- `P_DATE_RESPONSE` (Date, Input) - Дата/время ответа
- `P_ERROR_TEXT` (VarChar, Input, опционально) - Текст ошибки
- `P_ERR_CODE` (Number, Output) - Код ошибки процедуры
- `P_ERR_DESC` (VarChar, Output) - Описание ошибки процедуры

**Пример реализации callback для сохранения:**

```go
// В существующем коде Oracle адаптера

func SaveSMSResponse(event models.FireAqEvent) error {
    // Вызов процедуры Oracle
    // pcsystem.pkg_sms.save_sms_response(
    //     P_SMS_TASK_ID: event.TaskID,
    //     P_MESSAGE_ID: event.ReceiptedMessageID,
    //     P_STATUS_ID: event.MessageState,
    //     P_DATE_RESPONSE: event.ResponseDate,
    //     P_ERROR_TEXT: event.ErrorText
    // )
    
    // Обработка ошибок процедуры
    // if P_ERR_CODE != 0 {
    //     return fmt.Errorf("Oracle error: %s", P_ERR_DESC)
    // }
    
    return nil
}
```

**Особенности обработки delivery receipts:**

При получении delivery receipt от SMPP-сервера:
- `TaskID = -1` (связь с исходным сообщением происходит по `ReceiptedMessageID`)
- `MessageState = 4` если доставлено, `3` если ошибка доставки
- Oracle процедура должна найти исходное сообщение по `ReceiptedMessageID` и обновить его статус

### 8. Конфигурационный файл (config.ini)

```ini
[SMPP]
Host = smpp.smsc.ru
Port = 3700
User = asklepius
Password = askl7194
EnquireLinkInterval = 0
ReBindInterval = 0

[SMPP1]
Host = smpp.smsc.ru
Port = 3700
User = asklepius
Password = askl7194
EnquireLinkInterval = 0
ReBindInterval = 0

[Schedule]
TimeStart = 9:00
TimeEnd = 21:00
RebindSMPPMin = 60
ReloadQueueMin = 60
RebuildQueue = False
RebindOnClose = False

[Mode]
Debug = True
Silent = True

[Log]
LogLevel = 2
MaxArchiveFiles = 7
```

### 8. Особенности реализации на Golang

#### 8.1 Библиотеки SMPP для Golang

**Вариант 1: github.com/fiorix/go-smpp**
- Популярная библиотека
- Поддержка SMPP 3.4
- Требует доработки для полной поддержки всех функций

**Вариант 2: github.com/linxGnu/gosmpp**
- Более современная библиотека
- Лучшая поддержка TLV параметров
- Активная разработка

#### 8.2 Контексты и отмена операций

- Использование `context.Context` для отмены операций
- Graceful shutdown через `context.WithCancel`
- Таймауты для всех операций с SMPP

#### 8.3 Горутины и синхронизация

- Использование `sync.Mutex` для критических секций
- `sync.RWMutex` для чтения конфигурации
- Каналы для передачи сообщений (если требуется)

#### 8.4 Обработка ошибок

- Возврат ошибок вместо исключений
- Использование `fmt.Errorf` с `%w` для wrapping ошибок
- Retry-логика с экспоненциальной задержкой

#### 8.5 Парсинг delivery receipts

Delivery receipts приходят в формате TLV (Tag-Length-Value). Необходимо парсить:
- `receipted_message_id` - ID исходного сообщения
- `message_state` - статус доставки (0x02 = Delivered)
- Другие опциональные поля

### 9. Тестирование

#### 9.1 Unit-тесты

- Тесты парсинга конфигурации
- Тесты формирования PDU
- Тесты обработки delivery receipts

#### 9.2 Интеграционные тесты

- Тесты подключения к SMPP (mock SMPP сервер)
- Тесты отправки SMS
- Тесты обработки delivery receipts

#### 9.3 Тестовый контур

- Использование режима Debug для замены номеров
- Использование режима Silent для отключения отправки
- Mock-объекты для изоляции компонентов

### 10. Отличия от C# реализации

1. **Ошибки:** Возврат ошибок вместо исключений
2. **Контекст:** `context.Context` для отмены операций
3. **Синхронизация:** `sync.Mutex` вместо `Monitor`
4. **Библиотеки:** Другие библиотеки SMPP (go-smpp вместо AberrantSMPP)
5. **Типы:** Строгая типизация в Go

### 11. Рекомендации по реализации

**Учитывая, что работа с Oracle уже реализована:**

1. **Интеграция с существующим кодом:**
   - Использовать callback-механизм для сохранения результатов в Oracle
   - Не дублировать логику работы с Oracle очередью
   - Использовать уже распарсенные данные из `SMSMessage`

2. **Выбор библиотеки SMPP:**
   - Протестировать обе библиотеки (`github.com/fiorix/go-smpp` и `github.com/linxGnu/gosmpp`)
   - Выбрать библиотеку с лучшей поддержкой TLV параметров для delivery receipts
   - Убедиться в поддержке SMPP 3.4 и всех необходимых функций

3. **Обработка delivery receipts:**
   - Убедиться, что библиотека правильно парсит TLV в delivery receipts
   - Реализовать связь delivery receipt с исходным сообщением по `ReceiptedMessageID`
   - Обработать случай `TaskID = -1` при сохранении delivery receipt в Oracle

4. **Тестирование:**
   - Использовать режим `Silent = True` для тестирования без реальной отправки
   - Использовать режим `Debug = True` для отправки на тестовый номер
   - Обязательно протестировать интеграцию с существующей логикой Oracle

5. **Логирование и мониторинг:**
   - Использовать структурированное логирование (zap или аналогичное)
   - Логировать все этапы обработки: получение из Oracle, отправка, результат
   - Добавить метрики для отслеживания:
     - Количество отправленных SMS
     - Количество ошибок
     - Время обработки сообщений
     - Статус подключения к SMPP-серверам

6. **Обработка ошибок:**
   - Все ошибки должны сохраняться в Oracle с корректным статусом
   - Реализовать автоматическое переподключение при разрыве соединения
   - Обработать случаи недоступности SMPP-провайдера

7. **Производительность:**
   - Учитывать, что обработка сообщений из Oracle уже может быть параллельной
   - SMPP-адаптер должен быть потокобезопасным для параллельной отправки
   - Использовать пул соединений, если требуется высокая производительность

### 12. Чек-лист реализации

**Этапы реализации:**

- [ ] Выбрать и интегрировать библиотеку SMPP
- [ ] Реализовать `SMPPAdapter` с методами `Bind`, `Unbind`, `SendSMS`
- [ ] Реализовать обработку delivery receipts
- [ ] Создать `SMSService` с методом `ProcessSMS`
- [ ] Реализовать проверку расписания (`checkSchedule`)
- [ ] Реализовать режимы Debug и Silent
- [ ] Интегрировать с существующим кодом обработки Oracle очереди
- [ ] Настроить callback для сохранения результатов в Oracle
- [ ] Реализовать обработку ошибок и переподключение
- [ ] Добавить логирование и мониторинг
- [ ] Протестировать на тестовом контуре
- [ ] Протестировать интеграцию с Oracle

---

## Заключение

Данный план описывает реализацию отправки SMS через SMPP на Golang с учетом того, что работа с Oracle очередью уже реализована. Основные задачи:

**Уже реализовано:**
- ✅ Работа с Oracle AQ очередью
- ✅ Парсинг XMLType данных
- ✅ Структура данных `SMSMessage` с распарсенными полями

**Требуется реализовать:**
- ⏳ Интеграция с SMPP-сервером
- ⏳ Отправка SMS через SMPP
- ⏳ Обработка delivery receipts
- ⏳ Сохранение результатов в Oracle через callback
- ⏳ Управление подключением к SMPP

**Ключевые компоненты:**
- Конфигурация множественных SMPP-провайдеров
- Отправка SMS с поддержкой кириллицы (UCS2)
- Обработка delivery receipts с правильной связью по `ReceiptedMessageID`
- Тестовый контур (Debug и Silent режимы)
- Управление подключением и автоматическое переподключение
- Интеграция с существующей логикой Oracle через callback-механизм
