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
# Проверяем в стандартных местах для systemd unit файлов
for service in apache-pulsar apache-pulsar-bookie apache-pulsar-proxy apache-pulsar-functions-worker apache-pulsar-websocket; do
    if [ -f "/lib/systemd/system/${service}.service" ] || \
       [ -f "/usr/lib/systemd/system/${service}.service" ] || \
       [ -f "/etc/systemd/system/${service}.service" ]; then
        echo "✓ ${service}.service существует"
    else
        echo "✗ ${service}.service отсутствует"
    fi
done

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

# Проверка версии команд (без запуска сервисов)
echo ""
echo "=== Проверка команд ==="
if /usr/bin/pulsar --version >/dev/null 2>&1 || /usr/bin/pulsar version >/dev/null 2>&1; then
    echo "✓ Команда pulsar работает"
else
    echo "⚠ Команда pulsar не отвечает (может требовать конфигурацию)"
fi

if /usr/bin/pulsar-admin --version >/dev/null 2>&1 || /usr/bin/pulsar-admin version >/dev/null 2>&1; then
    echo "✓ Команда pulsar-admin работает"
else
    echo "⚠ Команда pulsar-admin не отвечает (может требовать конфигурацию)"
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