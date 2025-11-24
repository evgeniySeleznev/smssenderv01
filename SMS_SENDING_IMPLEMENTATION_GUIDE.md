# Инструкция по реализации отправки SMS через SMPP для Golang

## Обзор

Данный документ описывает реализацию отправки SMS через протокол SMPP на основе анализа C# сервиса `sms_sender`. Инструкция предназначена для реализации аналогичной функциональности в Golang сервисе.

## Параметры подключения к SMPP серверу

### Базовые параметры подключения

```ini
[SMPP]
Host = smpp.smsc.ru
Port = 3700
User = asklepius
Password = askl7194
```

**Параметры:**
- **Host**: Адрес SMPP сервера (например, `smpp.smsc.ru`)
- **Port**: Порт SMPP сервера (например, `3700`)
- **User**: Логин для подключения (System ID)
- **Password**: Пароль для подключения

### Опциональные параметры

```ini
#DestTon=          # Type of Number для адреса получателя (опционально)
#DestNpi=          # Numbering Plan Indicator для адреса получателя (опционально)
#SourceTon=        # Type of Number для адреса отправителя (опционально)
#SourceNpi=         # Numbering Plan Indicator для адреса отправителя (опционально)
#EnquireLinkInterval=0  # Интервал отправки ENQUIRE_LINK пакетов в секундах (0-1000)
#ReBindInterval=0       # Интервал переподключения в секундах (0-5000)
```

## Параметры SMPP Bind (подключения)

### Обязательные параметры

При создании SMPP клиента необходимо установить следующие параметры:

| Параметр | Значение | Описание |
|----------|----------|----------|
| **BindType** | `BindAsTransceiver` | Тип подключения - двусторонняя связь (отправка и прием) |
| **NpiType** | `National` | Национальный план нумерации |
| **TonType** | `International` | Международный тип номера |
| **Version** | `Version3_4` | Версия протокола SMPP 3.4 |

### Пример инициализации клиента (C#)

```csharp
client = new SMPPCommunicator
{
    Host = SmppHost,
    Port = SmppPort,
    SystemId = SmppUser,
    Password = SmppPassword,
    
    EnquireLinkInterval = pEnquireLinkInterval,  // интервал отправки пакетов поддержки соединения (ENQUIRE_LINK) в секундах
    ReBindInterval = pReBindInterval,
    
    BindType = SmppBind.BindingType.BindAsTransceiver,
    NpiType = Pdu.NpiType.National,
    TonType = Pdu.TonType.International,
    Version = Pdu.SmppVersionType.Version3_4
};
```

## Процесс подключения (Bind)

### Алгоритм подключения

1. **Проверка существующего подключения**: Если клиент уже подключен (`Bound == true`), сначала выполнить `Unbind()`
2. **Выполнение Bind**: Вызвать метод `Bind()` для установки соединения
3. **Проверка результата**: Убедиться, что подключение установлено успешно

### Пример реализации Bind (C#)

```csharp
public bool Bind()
{
    if (client == null) return false;
    try
    {
        LogAdapter.Debug("Bind(): " + GetInfo());
        if (client.Bound)
        {
            LogAdapter.Debug("  Unbind(): сброс подключения. " + GetInfo());
            client.Unbind();
        }
        if (client.Bind())
            LogAdapter.Debug("  Bind(): подключен. " + GetInfo());
        else
            throw new Exception("  Не удалось подключиться.");
    }
    catch (Exception e)
    {
        LogAdapter.Error(e, "Bind(): " + GetInfo());
        return false;
    }
    return true;
}
```

## Отправка SMS

### Параметры отправки SMS

При создании PDU `SubmitSM` необходимо установить следующие параметры:

| Параметр | Значение | Описание |
|----------|----------|----------|
| **AlertOnMsgDelivery** | `0x1` | Уведомление о доставке сообщения |
| **DataCoding** | `UCS2` | Кодировка текста - UCS-2 (Unicode) |
| **SourceAddress** | `senderName` | Имя отправителя (например, "SMS Service") |
| **DestinationAddress** | `"+7" + number` | Номер получателя с префиксом "+7" |
| **LanguageIndicator** | `Unspecified` | Индикатор языка (не указан) |
| **ShortMessage** | `text` | Текст сообщения |
| **PriorityFlag** | `Highest` | Высокий приоритет |
| **RegisteredDelivery** | `OnSuccessOrFailure` | Запрос статуса доставки (успех или ошибка) |

### Опциональные параметры адресов

Если в конфигурации указаны `DestTon`, `DestNpi`, `SourceTon`, `SourceNpi`, их необходимо применить:

```csharp
if (SourceNpi != null) req.SourceAddressNpi = SourceNpi ?? req.SourceAddressNpi;
if (SourceTon != null) req.SourceAddressTon = SourceTon ?? req.SourceAddressTon;
if (DestNpi != null) req.DestinationAddressNpi = DestNpi ?? req.DestinationAddressNpi;
if (DestTon != null) req.DestinationAddressTon = DestTon ?? req.DestinationAddressTon;
```

### Форматирование номера телефона

**Важно**: Номер получателя должен быть в формате `"+7" + number`, где `number` - номер без префикса.

Пример:
- Входной номер: `"9123456789"`
- Результирующий адрес: `"+79123456789"`

### Метод отправки

Используется метод с автоматической сегментацией длинных сообщений:

```csharp
smsId = client.Send(req, SmppSarMethod.UseSmppSegmentation).First();
```

`UseSmppSegmentation` - автоматически разбивает длинные сообщения на несколько частей (SAR - Segmentation And Reassembly).

### Полный пример метода SendSms (C#)

```csharp
public bool SendSms(string number, string text, string senderName, out string smsId, SmppAdapter smpp)
{
    smsId = null;

    // Проверка режима Silent (блокирует реальную отправку)
    if (Config.SilentMode)
    {
        LogAdapter.Debug(String.Concat("SMS не отправляется. SilentMode = true"));
        return true;
    }

    SmppSubmitSm req = null;
    try
    {
        // В режиме Debug номер заменяется на тестовый из БД
        if (Config.DebugMode)
        {
            if (!OracleAdapter.LockGetTestPhone(out number)) return false;
        }

        // Создание PDU для отправки
        req = new SmppSubmitSm()
        {
            AlertOnMsgDelivery = 0x1,
            DataCoding = DataCoding.UCS2,
            SourceAddress = senderName,
            DestinationAddress = "+7" + number,
            LanguageIndicator = LanguageIndicator.Unspecified,
            ShortMessage = text,
            PriorityFlag = Pdu.PriorityType.Highest,
            RegisteredDelivery = Pdu.RegisteredDeliveryType.OnSuccessOrFailure
        };

        // Применение опциональных параметров адресов
        if (SourceNpi != null) req.SourceAddressNpi = SourceNpi ?? req.SourceAddressNpi;
        if (SourceTon != null) req.SourceAddressTon = SourceTon ?? req.SourceAddressTon;
        if (DestNpi != null) req.DestinationAddressNpi = DestNpi ?? req.DestinationAddressNpi;
        if (DestTon != null) req.DestinationAddressTon = DestTon ?? req.DestinationAddressTon;

        // Отправка с автоматическим переподключением при ошибке
        try
        {
            if (!client.Connected) Bind();
            smsId = client.Send(req, SmppSarMethod.UseSmppSegmentation).First();
        }
        catch (Exception ex)
        {
            LogAdapter.Warn(ex, "SendSms(): подключение " + GetInfo());
            if (Bind())
                smsId = client.Send(req, SmppSarMethod.UseSmppSegmentation).First();
            else
                throw new Exception("SendSms(): ошибка подключения");
        }
        
        LogAdapter.Info(String.Concat("SMS отправлено: Sender: ", senderName, ", Number: ", number, ", Text: ", text, ", MessageId: ", smsId));
    }
    catch (Exception e)
    {
        LogAdapter.Error(String.Concat("Ошибка отправки SMS:", e.Message, " Sender: ", senderName, ", Number: ", number, ", Text: ", text));
        return false;
    }
    finally
    {
        req?.Dispose();
        req = null;
    }
    return true;
}
```

## Обработка ошибок и переподключение

### Автоматическое переподключение при отправке

При ошибке соединения во время отправки SMS выполняется автоматическое переподключение:

1. Ловится исключение при отправке
2. Выполняется `Bind()` для переподключения
3. Повторяется попытка отправки

### Проверка соединения перед отправкой

Перед каждой отправкой проверяется состояние соединения:

```csharp
if (!client.Connected) Bind();
```

## Обработка Delivery Receipts (статусы доставки)

### Настройка обработчика

Необходимо зарегистрировать обработчик события `OnDeliverSm` для получения статусов доставки:

```csharp
client.OnDeliverSm += OnDeliverSm;
```

### Обработчик Delivery Receipt

```csharp
void OnDeliverSm(object source, AberrantSMPP.EventObjects.DeliverSmEventArgs e)
{
    SmppDeliverSmResp smppDeliverSmResp = null;
    bool locked = false;
    try
    {
        locked = Monitor.TryEnter(_lock);
        if (locked)
        {
            smppDeliverSmResp = new SmppDeliverSmResp();
            _lastAnswerTime = DateTime.Now;
            smppDeliverSmResp.SequenceNumber = e.ResponsePdu.SequenceNumber;
            
            try
            {
                if (!client.Connected) Bind();
                client.SendPdu(smppDeliverSmResp);
            }
            catch (System.InvalidOperationException ex)
            {
                LogAdapter.Warn(ex, "SendPdu(): подключение " + GetInfo());
                if (Bind())
                {
                    client.SendPdu(smppDeliverSmResp);
                }
                else
                    throw new Exception("SendPdu(): ошибка подключения");
            }
        }
    }
    catch (Exception exception)
    {
        LogAdapter.Error(exception, "SendPdu(): " + GetInfo());
    }
    finally
    {
        if (locked) Monitor.Exit(_lock);
        smppDeliverSmResp?.Dispose();
        smppDeliverSmResp = null;
    }

    // Обработка статуса доставки
    try
    {
        if (locked && (e.DeliverSmPdu.MessageState == Pdu.MessageStateType.Delivered))
        {
            // SMS доставлено успешно
            SMS_Service.EnqueueAnswer(-1, e.DeliverSmPdu.ReceiptedMessageId, 4, e.ResponsePdu.SequenceNumber, "", DateTime.Now);
        } 
        else
        {
            // SMS не доставлено
            SMS_Service.EnqueueAnswer(-1, e.DeliverSmPdu.ReceiptedMessageId, 3, e.ResponsePdu.SequenceNumber, "SMS не доставлено до абонента", DateTime.Now);
        }
    }
    catch (Exception exception)
    {
        LogAdapter.Error(exception, "Ошибка OnDeliverSm()");
    }
    finally
    {
        e?.ResponsePdu?.Dispose();
        e?.DeliverSmPdu?.Dispose();
        e = null;
    }
}
```

### Статусы доставки

- **MessageState = Delivered** (4) - SMS доставлено успешно
- **MessageState != Delivered** (3) - SMS не доставлено

### Важно

1. **Обязательно отправлять ответ**: После получения `DeliverSm` необходимо отправить `DeliverSmResp` с тем же `SequenceNumber`
2. **Синхронизация**: Использовать блокировку (mutex/lock) при обработке delivery receipts
3. **Обновление времени последнего ответа**: Обновлять `_lastAnswerTime` при получении delivery receipt

## Переподключение (Rebind)

### Периодическое переподключение

Реализован механизм периодического переподключения для поддержания соединения:

```csharp
public bool LockRebind(uint RebindIntervalMin = 60)
{
    // Переподключение выполняется, если прошло более RebindIntervalMin минут
    // с момента последнего ответа от сервера
    if (client == null || _lastAnswerTime.AddMinutes(RebindIntervalMin) > DateTime.Now) return true;

    bool locked = false;
    bool res = false;
    try
    {
        locked = Monitor.TryEnter(_lock);
        if (locked)
        {
            LogAdapter.Debug("Rebind(): " + GetInfo());
            if (client.Bound)
            {
                LogAdapter.Debug("  Unbind(): сброс подключения. " + GetInfo());
                client.Unbind();
            }
            if (client.Bind())
            {
                LogAdapter.Debug("  Bind(): подключен. " + GetInfo());
                _lastAnswerTime = DateTime.Now;
                res = true;
            }
            else
                throw new Exception("  Не удалось подключиться.");
        }
    }
    catch (Exception e)
    {
        LogAdapter.Error(e, "Rebind() ошибка переподключения: " + GetInfo());
        res = false;
    }
    finally
    {
        if (locked) Monitor.Exit(_lock);
    }
    return res;
}
```

**Параметры:**
- `RebindIntervalMin` - интервал в минутах (по умолчанию 60)
- Переподключение выполняется, если с момента последнего ответа (`_lastAnswerTime`) прошло более указанного интервала

### Автоматическое переподключение при разрыве соединения

При разрыве соединения можно настроить автоматическое переподключение:

```csharp
client.OnClose += (s, e) =>
{
    LogAdapter.Warn("SMPP: OnClose(), " + GetInfo());
    if (Config.RebindOnClose)
        Bind();
};
```

## Отключение (Unbind)

### Корректное отключение

```csharp
public void Unbind()
{
    if (client == null || !client.Bound)
    {
        LogAdapter.Debug("Unbind() не требуется: " + GetInfo());
        return;
    }
    LogAdapter.Debug("Unbind() " + GetInfo());
    try
    {
        client.Unbind();
        if (IsConnected) throw new Exception("Нет отключения ");
        LogAdapter.Debug("   Отключились " + GetInfo());
    }
    catch (Exception e)
    {
        LogAdapter.Error(e, "Unbind(): " + GetInfo());
    }
}
```

## Рекомендации для реализации в Golang

### Используемые библиотеки

Рекомендуется использовать библиотеку `github.com/fiorix/go-smpp` или аналогичную для работы с SMPP протоколом в Golang.

### Структура данных конфигурации

```go
type SMPPConfig struct {
    Host              string
    Port              uint16
    User              string
    Password          string
    EnquireLinkInterval int  // 0-1000 секунд
    ReBindInterval      int  // 0-5000 секунд
    DestTon            *int  // опционально
    DestNpi            *int  // опционально
    SourceTon          *int  // опционально
    SourceNpi          *int  // опционально
}
```

### Основные функции для реализации

1. **Инициализация клиента** с параметрами Bind
2. **Метод Bind()** для подключения
3. **Метод Unbind()** для отключения
4. **Метод SendSMS()** для отправки SMS с параметрами из раздела "Отправка SMS"
5. **Обработчик Delivery Receipts** для получения статусов доставки
6. **Метод Rebind()** для периодического переподключения
7. **Обработка ошибок** с автоматическим переподключением

### Важные моменты

1. **Кодировка**: Использовать UCS-2 (UTF-16) для текста сообщений
2. **Формат номера**: Добавлять префикс "+7" к номеру получателя
3. **Сегментация**: Использовать автоматическую сегментацию для длинных сообщений
4. **Синхронизация**: Использовать `sync.Mutex` для защиты критических секций
5. **Обработка ошибок**: Реализовать автоматическое переподключение при ошибках соединения
6. **Delivery Receipts**: Обязательно отправлять ответ на `DeliverSm` PDU

### Пример структуры адаптера (Golang)

```go
type SMPPAdapter struct {
    client        *smpp.Transceiver
    config        *SMPPConfig
    mu            sync.Mutex
    lastAnswerTime time.Time
    destTon       *int
    destNpi       *int
    sourceTon     *int
    sourceNpi     *int
}

func NewSMPPAdapter(config *SMPPConfig) (*SMPPAdapter, error) {
    // Инициализация клиента с параметрами Bind
    // BindType: Transceiver
    // Version: SMPP 3.4
    // NpiType: National
    // TonType: International
}

func (a *SMPPAdapter) Bind() error {
    // Реализация подключения с проверкой существующего соединения
}

func (a *SMPPAdapter) SendSMS(number, text, senderName string) (string, error) {
    // Реализация отправки SMS с параметрами:
    // - DataCoding: UCS2
    // - DestinationAddress: "+7" + number
    // - RegisteredDelivery: OnSuccessOrFailure
    // - PriorityFlag: Highest
    // - AlertOnMsgDelivery: 0x1
    // - Использование сегментации для длинных сообщений
    // - Автоматическое переподключение при ошибке
}
```

## Заключение

Данная инструкция содержит все необходимые параметры и алгоритмы для реализации отправки SMS через SMPP протокол в Golang сервисе на основе анализа существующего C# сервиса. Основные моменты:

- Параметры подключения и Bind
- Параметры отправки SMS
- Обработка ошибок и переподключение
- Обработка delivery receipts
- Периодическое переподключение

Все параметры и значения взяты из реального рабочего кода C# сервиса.

