# Тестирование deb пакета Apache Pulsar

Этот Dockerfile используется для тестирования собранного deb пакета Apache Pulsar в изолированном окружении.

## Использование

1. Соберите deb пакет (используя `docker/deb/build/Dockerfile`)

2. Скопируйте deb пакет в эту директорию:
   
   ```bash
   cp /path/to/apache-pulsar_*.deb docker/deb/test/
   ```

3. Соберите тестовый образ:
   ```bash
   cd docker/deb/test
   docker build -t apache-pulsar-test .
   ```

4. Запустите тесты:
  
   ```bash
   docker run --rm --privileged apache-pulsar-test
   ```
   
   Или с монтированием deb файла:
  
   ```bash
   docker run --rm --privileged -v /path/to/deb:/test:ro apache-pulsar-test
   ```

## Что проверяется

### Базовая проверка установки
- Установка пакета через `dpkg`
- Наличие всех файлов и директорий
- Наличие systemd сервисов
- Создание системного пользователя `apache-pulsar`
- Права доступа на директории
- Исполняемость скриптов

### Проверка структуры файлов
- Наличие JAR файлов в `/usr/lib/apache-pulsar/lib/`
- Наличие скриптов в `/usr/lib/apache-pulsar/bin/`
- Наличие конфигурационных файлов в `/etc/apache-pulsar/`
- Корректность симлинка `/usr/lib/apache-pulsar/conf` → `/etc/apache-pulsar`
- Наличие символических ссылок в `/usr/bin/`

### Проверка команд
- `pulsar version` - проверка версии (исправлено в 4.1.2-10)
- `pulsar-admin --help` - проверка административной CLI
- `pulsar-client --help` - проверка клиента
- `pulsar-perf --help` - проверка инструмента производительности
- `bookkeeper --help` - проверка BookKeeper утилиты

### Тестирование standalone режима
- Запуск `pulsar standalone` (исправлено в 4.1.2-10)
- Проверка доступности HTTP API (порт 8080)
- Проверка доступности broker порта (6650)
- Проверка работы `pulsar-admin` с запущенным standalone сервером
- Проверка создания топиков и отправки/получения сообщений

### Проверка зависимостей
- Установленные зависимости из `debian/control`
- Наличие Java (openjdk-21-jre-headless или openjdk-17-jre-headless)
- Наличие необходимых системных библиотек

### Проверка man страниц
- Наличие man страниц для основных команд
- Доступность через команду `man`

## Структура тестов

Тесты выполняются в следующем порядке:

1. **Проверка установки пакета** - базовая проверка установки
2. **Проверка файловой структуры** - проверка всех директорий и файлов
3. **Проверка команд** - тестирование основных команд
4. **Тестирование standalone режима** - запуск и проверка standalone Pulsar
5. **Проверка зависимостей** - проверка установленных пакетов
6. **Проверка man страниц** - проверка документации

## Примечания

- Для полного тестирования systemd требуется запуск с `--privileged`
- Standalone режим запускается в фоновом режиме и автоматически останавливается после тестов
- Тесты используют временные директории (`/tmp/pulsar-standalone-test`) для изоляции
- Все тесты выполняются автоматически при запуске контейнера
- При ошибках тесты выводят подробную диагностическую информацию

## Известные исправления в версии 4.1.2-10

- Исправлено разрешение симлинков для скрипта `pulsar` - теперь команды `pulsar version` и `pulsar standalone` работают корректно
- Исправлена структура конфигурационных файлов - файлы находятся напрямую в `/etc/apache-pulsar/`, а не в поддиректории
- Добавлена проверка корректности симлинка `/usr/lib/apache-pulsar/conf` → `/etc/apache-pulsar`

## Устранение проблем

### Ошибка "ClassNotFoundException"

Если команды `pulsar version` или `pulsar standalone` выдают ошибку `ClassNotFoundException`, проверьте:
- Версию пакета (должна быть 4.1.2-10 или новее)
- Корректность симлинка `/usr/lib/apache-pulsar/conf` → `/etc/apache-pulsar`
- Наличие JAR файлов в `/usr/lib/apache-pulsar/lib/`

### Ошибка "FileNotFoundException: standalone.conf"

Если standalone режим не может найти конфигурационный файл:
- Проверьте наличие файла `/etc/apache-pulsar/standalone.conf`
- Проверьте корректность симлинка `/usr/lib/apache-pulsar/conf` → `/etc/apache-pulsar`

### Проблемы с правами доступа

Убедитесь, что системный пользователь `apache-pulsar` создан корректно:
```bash
id apache-pulsar
ls -la /usr/lib/apache-pulsar
ls -la /etc/apache-pulsar
```

## См. также

- `docker/deb/build/README.md` - сборка deb пакета
- `debian/README` - документация по структуре deb пакета