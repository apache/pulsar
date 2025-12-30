# Dockerfile для тестирования собранного deb пакета Apache Pulsar
# Используем ту же версию Debian, что и при сборке
FROM debian:trixie

# Обновление и установка зависимостей для тестирования
RUN apt-get update && apt-get install -y \
    systemd \
    systemd-sysv \
    openjdk-21-jre-headless \
    python3 \
    bash \
    adduser \
    libsnappy1v5 \
    zlib1g \
    libzstd1 \
    libc6 \
    libssl3 \
    ca-certificates \
    dpkg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Настройка systemd для работы в контейнере
# systemd требует, чтобы контейнер запускался с --privileged или --cap-add SYS_ADMIN
RUN systemctl set-default multi-user.target || true

# Создаем рабочую директорию для deb файлов
WORKDIR /test

# Скрипт для проверки установки (создаем в /usr/local/bin, чтобы не перезаписывался при монтировании)
RUN cat > /usr/local/bin/test-package.sh << 'EOFSCRIPT'
#!/bin/bash
set -e

echo "=== Тестирование deb пакета Apache Pulsar ==="

# Проверка наличия deb файла
DEB_FILE=$(ls /test/*.deb 2>/dev/null | head -n1)
if [ -z "$DEB_FILE" ] || [ ! -f "$DEB_FILE" ]; then
    echo "ERROR: deb файл не найден в /test/"
    exit 1
fi

echo "Найден пакет: $DEB_FILE"

# Проверка структуры пакета
echo ""
echo "=== Проверка структуры пакета ==="
dpkg -c "$DEB_FILE" | head -20

# Установка пакета
echo ""
echo "=== Установка пакета ==="
if ! dpkg -i "$DEB_FILE"; then
    echo "Исправление зависимостей..."
    apt-get install -f -y
fi

# Проверка успешности установки
if ! dpkg -l | grep -q "^ii.*apache-pulsar"; then
    echo "ERROR: Пакет не установлен успешно!"
    exit 1
fi
echo "✓ Пакет установлен успешно"

# Проверка установленных файлов
echo ""
echo "=== Проверка установленных файлов ==="

# Проверка основных директорий
test -d /usr/lib/apache-pulsar && echo "✓ /usr/lib/apache-pulsar существует" || echo "✗ /usr/lib/apache-pulsar отсутствует"
test -d /usr/lib/apache-pulsar/bin && echo "✓ /usr/lib/apache-pulsar/bin существует" || echo "✗ /usr/lib/apache-pulsar/bin отсутствует"
test -d /usr/lib/apache-pulsar/lib && echo "✓ /usr/lib/apache-pulsar/lib существует" || echo "✗ /usr/lib/apache-pulsar/lib отсутствует"
test -d /etc/apache-pulsar && echo "✓ /etc/apache-pulsar существует" || echo "✗ /etc/apache-pulsar отсутствует"
test -d /var/log/apache-pulsar && echo "✓ /var/log/apache-pulsar существует" || echo "✗ /var/log/apache-pulsar отсутствует"

# Проверка основных файлов
test -f /usr/lib/apache-pulsar/bin/pulsar && echo "✓ /usr/lib/apache-pulsar/bin/pulsar существует" || echo "✗ /usr/lib/apache-pulsar/bin/pulsar отсутствует"
test -f /usr/lib/apache-pulsar/bin/pulsar-admin && echo "✓ /usr/lib/apache-pulsar/bin/pulsar-admin существует" || echo "✗ /usr/lib/apache-pulsar/bin/pulsar-admin отсутствует"
test -f /usr/lib/apache-pulsar/bin/bookkeeper && echo "✓ /usr/lib/apache-pulsar/bin/bookkeeper существует" || echo "✗ /usr/lib/apache-pulsar/bin/bookkeeper отсутствует"

# Проверка JAR файлов
JAR_COUNT=$(find /usr/lib/apache-pulsar -name "*.jar" 2>/dev/null | wc -l)
echo "✓ Найдено JAR файлов: $JAR_COUNT"
if [ "$JAR_COUNT" -eq 0 ]; then
    echo "✗ WARNING: JAR файлы не найдены!"
fi

# Проверка основного broker JAR файла
BROKER_JAR=$(ls /usr/lib/apache-pulsar/pulsar-broker*.jar 2>/dev/null | grep -v tests | head -1)
if [ -n "$BROKER_JAR" ]; then
    echo "✓ Основной broker JAR найден: $(basename "$BROKER_JAR")"
    # Проверяем, что JAR содержит нужные классы
    if unzip -l "$BROKER_JAR" 2>/dev/null | grep -q "org/apache/pulsar/PulsarStandaloneStarter"; then
        echo "✓ JAR содержит класс PulsarStandaloneStarter"
    else
        echo "⚠ WARNING: JAR не содержит класс PulsarStandaloneStarter"
        echo "  Проверяем содержимое JAR:"
        unzip -l "$BROKER_JAR" 2>/dev/null | grep -i "pulsar.*starter" | head -5 || true
    fi
    if unzip -l "$BROKER_JAR" 2>/dev/null | grep -q "org/apache/pulsar/PulsarVersionStarter"; then
        echo "✓ JAR содержит класс PulsarVersionStarter"
    else
        echo "⚠ WARNING: JAR не содержит класс PulsarVersionStarter"
    fi
else
    echo "✗ WARNING: Основной broker JAR не найден в /usr/lib/apache-pulsar/"
    echo "  Найденные JAR файлы в корне:"
    ls -la /usr/lib/apache-pulsar/*.jar 2>/dev/null | head -5 || echo "  Нет JAR файлов"
fi

# Проверка pulsar-admin-common.sh
if [ -f /usr/lib/apache-pulsar/bin/pulsar-admin-common.sh ]; then
    echo "✓ pulsar-admin-common.sh найден в /usr/lib/apache-pulsar/bin/"
else
    echo "✗ WARNING: pulsar-admin-common.sh не найден в /usr/lib/apache-pulsar/bin/"
fi

# Проверка определения PULSAR_HOME в скриптах
echo "--- Проверка скриптов ---"
if [ -f /usr/lib/apache-pulsar/bin/pulsar-admin ]; then
    # Проверяем, что скрипт правильно определяет PULSAR_HOME
    TEST_PULSAR_HOME=$(cd /usr/lib/apache-pulsar/bin && bash -c 'BINDIR=$(dirname "$(readlink -f pulsar-admin 2>/dev/null || echo pulsar-admin)"); export PULSAR_HOME=$(cd -P $BINDIR/..;pwd); echo $PULSAR_HOME')
    if [ "$TEST_PULSAR_HOME" = "/usr/lib/apache-pulsar" ]; then
        echo "✓ Скрипт pulsar-admin правильно определяет PULSAR_HOME: $TEST_PULSAR_HOME"
    else
        echo "⚠ WARNING: Скрипт pulsar-admin определяет PULSAR_HOME как: $TEST_PULSAR_HOME (ожидается: /usr/lib/apache-pulsar)"
    fi
fi

# Проверка lib директории
if [ -d /usr/lib/apache-pulsar/lib ]; then
    LIB_JAR_COUNT=$(find /usr/lib/apache-pulsar/lib -name "*.jar" 2>/dev/null | wc -l)
    echo "✓ Найдено JAR файлов в lib/: $LIB_JAR_COUNT"
    if [ "$LIB_JAR_COUNT" -eq 0 ]; then
        echo "⚠ WARNING: JAR файлы в lib/ не найдены!"
    fi
else
    echo "✗ WARNING: Директория lib/ не найдена!"
fi

# Проверка символических ссылок
echo ""
echo "=== Проверка символических ссылок ==="
test -L /usr/bin/pulsar && echo "✓ /usr/bin/pulsar существует" || echo "✗ /usr/bin/pulsar отсутствует"
test -L /usr/bin/pulsar-admin && echo "✓ /usr/bin/pulsar-admin существует" || echo "✗ /usr/bin/pulsar-admin отсутствует"
test -L /usr/bin/bookkeeper && echo "✓ /usr/bin/bookkeeper существует" || echo "✗ /usr/bin/bookkeeper отсутствует"

# Проверка конфигурационных файлов
echo ""
echo "=== Проверка конфигурационных файлов ==="
CONF_COUNT=$(find /etc/apache-pulsar -name "*.conf" 2>/dev/null | wc -l)
echo "✓ Найдено конфигурационных файлов: $CONF_COUNT"

# Проверка systemd сервисов
echo ""
echo "=== Проверка systemd сервисов ==="
# Проверяем основной сервис в стандартных местах
if [ -f "/lib/systemd/system/apache-pulsar.service" ] || \
   [ -f "/usr/lib/systemd/system/apache-pulsar.service" ] || \
   [ -f "/etc/systemd/system/apache-pulsar.service" ]; then
    echo "✓ apache-pulsar.service существует"
else
    echo "✗ apache-pulsar.service отсутствует"
fi

# Проверяем дополнительные сервисы в examples директории
echo ""
echo "=== Проверка примеров systemd сервисов ==="
EXAMPLES_DIR="/usr/share/apache-pulsar/examples/systemd"
if [ -d "$EXAMPLES_DIR" ]; then
    echo "✓ Директория примеров существует: $EXAMPLES_DIR"
    for service in apache-pulsar-bookie apache-pulsar-proxy apache-pulsar-functions-worker apache-pulsar-websocket apache-pulsar-autorecovery; do
        if [ -f "$EXAMPLES_DIR/${service}.service" ]; then
            echo "✓ ${service}.service найден в примерах"
        else
            echo "✗ ${service}.service отсутствует в примерах"
        fi
    done
    EXAMPLE_COUNT=$(find "$EXAMPLES_DIR" -name "*.service" 2>/dev/null | wc -l)
    echo "✓ Найдено примеров сервисов: $EXAMPLE_COUNT"
else
    echo "✗ Директория примеров отсутствует: $EXAMPLES_DIR"
fi

# Проверка пользователя
echo ""
echo "=== Проверка системного пользователя ==="
if getent passwd apache-pulsar >/dev/null 2>&1; then
    echo "✓ Пользователь apache-pulsar создан"
    getent passwd apache-pulsar
else
    echo "✗ Пользователь apache-pulsar не создан"
fi

# Проверка прав доступа
echo ""
echo "=== Проверка прав доступа ==="
if [ -d /usr/lib/apache-pulsar ]; then
    OWNER=$(stat -c '%U:%G' /usr/lib/apache-pulsar)
    if [ "$OWNER" = "apache-pulsar:apache-pulsar" ]; then
        echo "✓ Права на /usr/lib/apache-pulsar корректны: $OWNER"
    else
        echo "✗ Права на /usr/lib/apache-pulsar некорректны: $OWNER (ожидается apache-pulsar:apache-pulsar)"
    fi
fi

# Проверка исполняемости скриптов
echo ""
echo "=== Проверка исполняемости скриптов ==="
test -x /usr/lib/apache-pulsar/bin/pulsar && echo "✓ pulsar исполняемый" || echo "✗ pulsar не исполняемый"
test -x /usr/lib/apache-pulsar/bin/pulsar-admin && echo "✓ pulsar-admin исполняемый" || echo "✗ pulsar-admin не исполняемый"

# Проверка функционала команд (без запуска сервисов)
echo ""
echo "=== Проверка команд ==="

# Проверка наличия Java
if command -v java >/dev/null 2>&1; then
    JAVA_VERSION=$(java -version 2>&1 | head -n1)
    echo "✓ Java доступна: $JAVA_VERSION"
else
    echo "✗ Java не найдена"
fi

# Установка переменных окружения для команд
export PULSAR_HOME=/usr/lib/apache-pulsar
export PULSAR_CONF=/etc/apache-pulsar
export PULSAR_LOG_DIR=/var/log/apache-pulsar

# Проверка pulsar - команда требует аргументы
echo ""
echo "--- Проверка pulsar ---"
PULSAR_OUTPUT=$(/usr/bin/pulsar 2>&1) || true
PULSAR_EXIT=$?
if [ $PULSAR_EXIT -eq 1 ] && echo "$PULSAR_OUTPUT" | grep -qiE "Usage|pulsar|command|broker|bookie"; then
    echo "✓ Команда pulsar работает (показывает usage при отсутствии аргументов)"
    # Показываем количество доступных команд
    COMMAND_COUNT=$(echo "$PULSAR_OUTPUT" | grep -cE "^\s+(broker|bookie|zookeeper|proxy|websocket|standalone)" 2>/dev/null || echo "0")
    if [ -n "$COMMAND_COUNT" ] && [ "$COMMAND_COUNT" -gt 0 ] 2>/dev/null; then
        echo "  Доступно команд: $COMMAND_COUNT"
    fi
elif /usr/bin/pulsar --help >/dev/null 2>&1; then
    echo "✓ Команда pulsar работает (--help)"
else
    echo "⚠ Команда pulsar: exit code=$PULSAR_EXIT"
    echo "  Вывод: $(echo "$PULSAR_OUTPUT" | head -n3 | tr '\n' ' ' 2>/dev/null || echo 'нет вывода')"
fi

# Проверка подкоманд pulsar (могут требовать конфигурацию, но должны показывать help)
if /usr/bin/pulsar broker --help >/dev/null 2>&1 || /usr/bin/pulsar broker --help 2>&1 | grep -qiE "Usage|help|broker"; then
    echo "✓ Команда 'pulsar broker' доступна"
fi

if /usr/bin/pulsar standalone --help >/dev/null 2>&1 || /usr/bin/pulsar standalone --help 2>&1 | grep -qiE "Usage|help|standalone"; then
    echo "✓ Команда 'pulsar standalone' доступна"
fi

# Проверка pulsar-admin
echo ""
echo "--- Проверка pulsar-admin ---"
PULSAR_ADMIN_OUTPUT=$(/usr/bin/pulsar-admin 2>&1) || true
PULSAR_ADMIN_EXIT=$?
if [ $PULSAR_ADMIN_EXIT -ne 0 ] && echo "$PULSAR_ADMIN_OUTPUT" | grep -qiE "Usage|pulsar-admin|command|help|clusters|tenants"; then
    echo "✓ Команда pulsar-admin работает (показывает usage при отсутствии аргументов)"
    # Показываем количество доступных подкоманд
    ADMIN_COMMAND_COUNT=$(echo "$PULSAR_ADMIN_OUTPUT" | grep -cE "^\s+(clusters|tenants|namespaces|topics|functions)" 2>/dev/null || echo "0")
    if [ -n "$ADMIN_COMMAND_COUNT" ] && [ "$ADMIN_COMMAND_COUNT" -gt 0 ] 2>/dev/null; then
        echo "  Доступно подкоманд: $ADMIN_COMMAND_COUNT"
    fi
elif /usr/bin/pulsar-admin --help >/dev/null 2>&1; then
    echo "✓ Команда pulsar-admin работает (--help)"
else
    echo "⚠ Команда pulsar-admin: exit code=$PULSAR_ADMIN_EXIT"
    echo "  Вывод: $(echo "$PULSAR_ADMIN_OUTPUT" | head -n3 | tr '\n' ' ' 2>/dev/null || echo 'нет вывода')"
fi

# Проверка подкоманд pulsar-admin (могут требовать подключения к брокеру)
if /usr/bin/pulsar-admin clusters --help >/dev/null 2>&1 || /usr/bin/pulsar-admin clusters list 2>&1 | grep -qiE "Usage|help|clusters|connection"; then
    echo "✓ Команда 'pulsar-admin clusters' доступна"
fi

if /usr/bin/pulsar-admin tenants --help >/dev/null 2>&1 || /usr/bin/pulsar-admin tenants list 2>&1 | grep -qiE "Usage|help|tenants|connection"; then
    echo "✓ Команда 'pulsar-admin tenants' доступна"
fi

# Проверка других команд
echo ""
echo "--- Проверка других команд ---"
if /usr/bin/bookkeeper --help >/dev/null 2>&1; then
    echo "✓ Команда bookkeeper работает (--help)"
elif BOOKIE_OUTPUT=$(/usr/bin/bookkeeper 2>&1 || true); echo "$BOOKIE_OUTPUT" | grep -qiE "Usage|bookkeeper|command"; then
    echo "✓ Команда bookkeeper работает (показывает usage)"
else
    echo "⚠ Команда bookkeeper не отвечает"
fi

if /usr/bin/pulsar-client --help >/dev/null 2>&1; then
    echo "✓ Команда pulsar-client работает (--help)"
elif CLIENT_OUTPUT=$(/usr/bin/pulsar-client 2>&1 || true); echo "$CLIENT_OUTPUT" | grep -qiE "Usage|pulsar-client|command"; then
    echo "✓ Команда pulsar-client работает (показывает usage)"
else
    echo "⚠ Команда pulsar-client не отвечает"
fi

echo ""
echo "Примечание: Команды могут требовать конфигурацию (ZooKeeper, BookKeeper) для полной функциональности."
echo "Проверка показывает, что команды установлены и могут быть запущены."

# Проверка запуска standalone Pulsar
echo ""
echo "=== Проверка запуска standalone Pulsar ==="

# Диагностика: проверяем переменные окружения и JAR файлы
echo "--- Диагностика перед запуском ---"
echo "PULSAR_HOME: ${PULSAR_HOME:-не установлен}"
if [ -n "$PULSAR_HOME" ]; then
    echo "JAR файлы в корне PULSAR_HOME:"
    ls -la "$PULSAR_HOME"/*.jar 2>/dev/null | head -5 || echo "  JAR файлы не найдены в корне"
    echo "JAR файлы в lib/:"
    ls -la "$PULSAR_HOME/lib"/*.jar 2>/dev/null | wc -l | xargs echo "  Найдено JAR файлов:"
    echo "Основной broker JAR:"
    ls -la "$PULSAR_HOME"/pulsar-broker*.jar 2>/dev/null || echo "  Не найден"
fi

echo "Запуск standalone Pulsar (может занять некоторое время)..."

# Создаем временную директорию для данных standalone
STANDALONE_DATA_DIR=/tmp/pulsar-standalone-test
mkdir -p "$STANDALONE_DATA_DIR/data"
mkdir -p "$STANDALONE_DATA_DIR/logs"

# Проверяем наличие конфигурационного файла standalone
if [ -f /etc/apache-pulsar/standalone.conf ]; then
    STANDALONE_CONF="/etc/apache-pulsar/standalone.conf"
    echo "Используется конфигурация: $STANDALONE_CONF"
else
    # Если конфигурации нет, standalone будет использовать значения по умолчанию
    STANDALONE_CONF=""
    echo "Конфигурация не найдена, используются значения по умолчанию"
fi

# Диагностика: проверяем, что скрипт может найти JAR
echo "--- Проверка скрипта pulsar ---"
# Запускаем скрипт с --version для проверки, что он находит JAR
if /usr/bin/pulsar version >/dev/null 2>&1; then
    echo "✓ Команда 'pulsar version' работает"
else
    VERSION_OUTPUT=$(/usr/bin/pulsar version 2>&1 || true)
    echo "⚠ Команда 'pulsar version' не работает:"
    echo "$VERSION_OUTPUT" | head -5
fi

# Запускаем standalone в фоне с указанием конфигурации если она есть
if [ -n "$STANDALONE_CONF" ]; then
    /usr/bin/pulsar standalone -c "$STANDALONE_CONF" > "$STANDALONE_DATA_DIR/logs/standalone.log" 2>&1 &
else
    /usr/bin/pulsar standalone > "$STANDALONE_DATA_DIR/logs/standalone.log" 2>&1 &
fi
PULSAR_PID=$!

echo "Standalone Pulsar запущен (PID: $PULSAR_PID)"
echo "Ожидание запуска (максимум 60 секунд)..."

# Ждем запуска - проверяем порт 8080 (HTTP admin API)
TIMEOUT=60
ELAPSED=0
STARTED=0

while [ $ELAPSED -lt $TIMEOUT ]; do
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    
    # Проверяем, что процесс еще работает
    if ! kill -0 $PULSAR_PID 2>/dev/null; then
        echo "⚠ Standalone Pulsar завершился неожиданно"
        echo "Последние строки лога:"
        tail -n 10 "$STANDALONE_DATA_DIR/logs/standalone.log" 2>/dev/null || true
        STARTED=0
        break
    fi
    
    # Проверяем HTTP API
    if curl -s -f http://localhost:8080/admin/v2/clusters/standalone >/dev/null 2>&1; then
        echo "✓ Standalone Pulsar запущен и отвечает на HTTP API (порт 8080)"
        STARTED=1
        break
    fi
    
    # Показываем прогресс каждые 10 секунд
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        echo "  Ожидание... ($ELAPSED/$TIMEOUT секунд)"
    fi
done

if [ $STARTED -eq 1 ]; then
    # Проверяем порт 6650 (broker)
    if timeout 1 bash -c "echo > /dev/tcp/localhost/6650" 2>/dev/null; then
        echo "✓ Broker порт 6650 открыт"
    else
        echo "⚠ Broker порт 6650 не отвечает"
    fi
    
    # Проверяем через pulsar-admin
    if /usr/bin/pulsar-admin clusters list 2>&1 | grep -q "standalone"; then
        echo "✓ Кластер 'standalone' доступен через pulsar-admin"
    fi
    
    # Проверяем namespaces
    if /usr/bin/pulsar-admin namespaces list public 2>&1 | grep -q "default"; then
        echo "✓ Namespace 'public/default' доступен"
    fi
    
    echo ""
    echo "✓ Standalone Pulsar успешно запущен и работает!"
    echo "  HTTP API: http://localhost:8080"
    echo "  Broker: pulsar://localhost:6650"
    
    # Останавливаем standalone
    echo ""
    echo "Остановка standalone Pulsar..."
    kill $PULSAR_PID 2>/dev/null || true
    sleep 3
    
    # Если процесс еще работает, принудительно завершаем
    if kill -0 $PULSAR_PID 2>/dev/null; then
        kill -9 $PULSAR_PID 2>/dev/null || true
        echo "  Принудительно завершен"
    else
        echo "✓ Standalone Pulsar остановлен"
    fi
    
    # Очистка временных данных
    rm -rf "$STANDALONE_DATA_DIR" 2>/dev/null || true
else
    echo "✗ Standalone Pulsar не запустился в течение $TIMEOUT секунд"
    if [ -f "$STANDALONE_DATA_DIR/logs/standalone.log" ]; then
        echo "Последние строки лога:"
        tail -n 20 "$STANDALONE_DATA_DIR/logs/standalone.log"
    fi
    # Останавливаем процесс если он еще работает
    kill $PULSAR_PID 2>/dev/null || true
    kill -9 $PULSAR_PID 2>/dev/null || true
    rm -rf "$STANDALONE_DATA_DIR" 2>/dev/null || true
fi

# Проверка man страниц (если есть)
echo ""
echo "=== Проверка man страниц ==="
if command -v man >/dev/null 2>&1; then
    if man -w pulsar >/dev/null 2>&1; then
        echo "✓ man страница pulsar установлена"
    else
        echo "⚠ man страница pulsar не найдена"
    fi
fi

# Проверка зависимостей пакета
echo ""
echo "=== Проверка установленных зависимостей ==="
dpkg -l | grep -E "(openjdk-|python3|bash|adduser|libsnappy|zlib|libzstd|libssl|ca-certificates)" | head -10

# Итоговый отчет
echo ""
echo "=== Итоговый отчет ==="
PACKAGE_INFO=$(dpkg -l | grep "^ii.*apache-pulsar" | awk '{print $2 " " $3}')
if [ -n "$PACKAGE_INFO" ]; then
    echo "Пакет установлен: $PACKAGE_INFO"
else
    echo "⚠ Пакет не найден в списке установленных"
fi
echo ""
echo "=== Тестирование завершено ==="
EOFSCRIPT

RUN chmod +x /usr/local/bin/test-package.sh && \
    sed -i 's/\r$//' /usr/local/bin/test-package.sh && \
    test -f /usr/local/bin/test-package.sh && \
    test -x /usr/local/bin/test-package.sh && \
    head -1 /usr/local/bin/test-package.sh | grep -q '^#!/bin/bash' && \
    echo "Script created successfully"

# Запускаем тесты при старте контейнера
# Используем явный вызов bash для гарантии работы
CMD ["/bin/bash", "-x", "/usr/local/bin/test-package.sh"]