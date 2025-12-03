# Инструкция по упаковке и деплою проекта SMS Sender

## Структура проекта

Проект подготовлен для автоматической сборки через Bamboo и деплоя в Harbor согласно регламенту.

### Структура файлов:

```
smsSender/
├── compose.yaml              # Сборочный compose файл (в корне проекта)
├── Dockerfile                # Dockerfile для сборки приложения
├── _installDir/              # Папка с установочными файлами
│   ├── compose.yaml          # Установочный compose файл
│   ├── README.md             # Инструкция по установке и тестированию
│   └── settings/
│       └── settings.ini.example  # Пример файла настроек
├── settings/                 # Папка с настройками (для разработки)
│   └── settings.ini       # Файл настроек (в .gitignore)
├── logs/                     # Папка для логов (в .gitignore)
├── main.go                   # Главный файл приложения
├── go.mod                    # Go модуль
├── go.sum                    # Go зависимости
└── [другие исходные файлы]
```

## Требования к проекту

### 1. Сборочный compose.yaml (в корне)

- Содержит секцию `build` для сборки образа
- Указывает образ в Harbor: `dockerrepo.progcom.ru/sms_sender/sms_sender:<tag>`
- Монтирует volumes с относительными путями от корня проекта
- **Важно:** Не используйте тег `latest`

### 2. Установочный compose.yaml (в _installDir)

- **НЕ содержит** секцию `build`
- Использует готовый образ из Harbor
- Монтирует те же volumes, что и сборочный compose

### 3. Dockerfile

- Двухэтапная сборка:
  1. Этап `build`: компиляция Go приложения
  2. Этап runtime: минимальный образ с Oracle Instant Client и скомпилированным бинарником

## Процесс сборки

1. **Коммит в Git:** После отправки проекта в Git хранилище автоматически запускается сборка в Bamboo

2. **Сборка образа:** Bamboo выполняет `docker compose build` используя корневой `compose.yaml`

3. **Загрузка в Harbor:** Собранный образ загружается в `dockerrepo.progcom.ru/sms_sender/sms_sender:<tag>`

4. **Результаты сборки:** Появляются в `\\assdocker\build\frombamboo\sms_sender\<версия>\`:
   - `installDir/` - установочные файлы
   - `docker_logs/` - логи сборки и загрузки

## Проверка перед отправкой в тест/прод

**Обязательно проверьте:**

1. ✅ Проект успешно собирается локально:
   ```bash
   docker compose build
   ```

2. ✅ Образ загружен в Harbor:
   - Проверьте в веб-интерфейсе Harbor: `dockerrepo.progcom.ru`
   - Или через команду: `docker pull dockerrepo.progcom.ru/sms_sender/sms_sender:0.0.1`

3. ✅ Установочный compose.yaml работает:
   ```bash
   cd _installDir
   docker compose pull
   docker compose up -d
   ```

4. ✅ Приложение запускается и подключается к БД (проверьте логи)

## Обновление версии

Для обновления проекта:

1. Измените тег в обоих `compose.yaml` файлах:
   ```yaml
   image: dockerrepo.progcom.ru/sms_sender/sms_sender:0.0.2
   ```

2. Закоммитьте изменения в Git

3. Сборка запустится автоматически

## Тестирование готового решения

Подробная инструкция по тестированию находится в файле `_installDir/README.md`

### Краткая проверка:

1. **Проверка образа в Harbor:**
   ```bash
   docker pull dockerrepo.progcom.ru/sms_sender/sms_sender:0.0.1
   ```

2. **Локальное тестирование:**
   ```bash
   cd _installDir
   # Создайте файл настроек из примера
   cp settings/settings.ini.example settings/settings.ini
   # Отредактируйте настройки
   # Создайте папку для логов
   mkdir -p logs
   # Запустите контейнер
   docker compose up -d
   # Проверьте логи
   docker compose logs -f
   ```

3. **Проверка работы:**
   - Контейнер должен быть в статусе `Up`
   - В логах должно быть сообщение "Успешно подключено к Oracle базе данных"
   - Приложение должно начать читать очередь Oracle AQ
   - Логи должны записываться в папку `logs/`

4. **Проверка graceful shutdown:**
   ```bash
   docker compose stop
   docker compose logs | tail -20
   ```

## Важные замечания

1. **Не используйте тег `latest`** - всегда указывайте конкретную версию

2. **Относительные пути:** Все пути в volumes должны быть относительными от корня проекта

3. **Разделение логики:** Dockerfile - для сборки, compose.yaml - для конфигурации и монтирования volumes

4. **Файлы настроек:** В `_installDir` должен быть пример файла настроек, реальный файл создается при установке

5. **Oracle Instant Client:** Образ содержит Oracle Instant Client, необходимый для работы драйвера godror

## Контакты и поддержка

При возникновении проблем:
1. Проверьте логи сборки в `docker_logs/`
2. Проверьте логи контейнера: `docker compose logs`
3. Убедитесь, что образ загружен в Harbor
4. Проверьте настройки подключения к БД и SMPP

