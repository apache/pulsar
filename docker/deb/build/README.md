# Сборка deb пакета Apache Pulsar

Этот Dockerfile используется для сборки deb пакета Apache Pulsar в изолированном окружении.

## Требования

- Docker
- Исходный код Apache Pulsar с директорией `debian/`

## Использование

### Быстрый старт

1. Убедитесь, что вы находитесь в корне проекта Apache Pulsar:

   ```bash
   cd /path/to/apache-pulsar-4.1.2
   ```

2. Соберите Docker образ:

   ```bash
   docker build -t apache-pulsar-builder -f docker/deb/build/Dockerfile .
   ```

3. Запустите сборку:

   **Linux/macOS:**
   ```bash
   docker run --rm -v $(pwd)/docker/deb/build/output:/mnt/deb apache-pulsar-builder
   ```

   **Windows PowerShell:**
   ```powershell
   docker run --rm -v ${PWD}/docker/deb/build/output:/mnt/deb apache-pulsar-builder
   ```

   **Windows CMD:**
   ```cmd
   docker run --rm -v %CD%/docker/deb/build/output:/mnt/deb apache-pulsar-builder
   ```

4. Собранный deb пакет и все файлы результатов будут автоматически скопированы в директорию `docker/deb/build/output/`:

   **Linux/macOS:**
   ```bash
   ls -lh docker/deb/build/output/
   ```

   **Windows PowerShell:**
   ```powershell
   Get-ChildItem docker\deb\build\output\
   ```

### Альтернативный способ (с интерактивным доступом)

Если нужно получить доступ к контейнеру после сборки:

**Linux/macOS:**
```bash
docker run -it --rm -v $(pwd)/docker/deb/build/output:/mnt/deb apache-pulsar-builder bash
```

**Windows PowerShell:**
```powershell
docker run -it --rm -v ${PWD}/docker/deb/build/output:/mnt/deb apache-pulsar-builder bash
```

Затем внутри контейнера файлы автоматически скопируются в `/mnt/deb/` после завершения сборки командой `debuild -us -uc -b`, или вы можете скопировать их вручную:

```bash
cp /workspace/*.deb /mnt/deb/
cp /workspace/*.changes /mnt/deb/
cp /workspace/*.buildinfo /mnt/deb/
```

## Что делает Dockerfile

1. **Устанавливает базовое окружение**: Debian Trixie с необходимыми инструментами сборки
2. **Устанавливает зависимости**: Все зависимости из `debian/control` (Build-Depends)
3. **Подготавливает исходники**: Создает правильную структуру для `debuild`:
   - Создает `.orig.tar.gz` архив без `debian/`
   - Добавляет `debian/` директорию для сборки
4. **Настраивает права**: Устанавливает правильные права на файлы
5. **Собирает пакет**: Запускает `debuild -us -uc -b`

## Переменные окружения

- `MAVEN_OPTS`: Опции для Maven (по умолчанию `-Xmx5G`)
- `JAVA_HOME`: Путь к JDK (по умолчанию `/usr/lib/jvm/default-java`)

## Структура сборки

```
/workspace/
├── apache-pulsar-4.1.2/          # Исходники с debian/
├── apache-pulsar_4.1.2.orig.tar.gz  # Upstream архив
└── *.deb                          # Собранные пакеты
```

## Выходные файлы

После успешной сборки в `/mnt/deb/` будут:

- `apache-pulsar_4.1.2-X_amd64.deb` - бинарный пакет
- `apache-pulsar_4.1.2-X_amd64.changes` - файл изменений
- `apache-pulsar_4.1.2-X_amd64.buildinfo` - информация о сборке

Где `X` - номер ревизии из `debian/changelog`.

## Устранение проблем

### Ошибка "No space left on device"

Увеличьте размер Docker диска или очистите неиспользуемые образы:

```bash
docker system prune -a
```

### Ошибка сборки Maven

Проверьте, что достаточно памяти для Maven. Можно изменить `MAVEN_OPTS`:

```bash
docker run --rm -e MAVEN_OPTS="-Xmx8G" -v $(pwd)/docker/deb/build/output:/mnt/deb apache-pulsar-builder
```

### Проблемы с правами доступа

Убедитесь, что директория вывода существует и доступна:

```bash
mkdir -p docker/deb/build/output
chmod 755 docker/deb/build/output
```

## Примечания

- Сборка может занять значительное время (30-60 минут в зависимости от системы)
- Требуется минимум 8GB свободной памяти для комфортной сборки
- Для сборки используется Debian Trixie (testing), что обеспечивает актуальные версии инструментов
- Пакет собирается без подписи (`-us -uc`). Для подписи нужно настроить GPG ключи

## Следующие шаги

После успешной сборки:

1. Протестируйте пакет используя `docker/deb/test/Dockerfile`
2. Установите пакет на тестовой системе
3. Проверьте работу всех сервисов

## См. также

- `docker/deb/test/README.md` - тестирование собранного пакета
- `debian/README` - документация по структуре deb пакета

