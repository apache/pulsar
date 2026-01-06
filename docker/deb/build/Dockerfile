# Используем Debian 12 Bookworm (стабильная версия)
# Если нужна именно Trixie, можно вернуть debian:trixie
FROM debian:trixie

# Обновление и установка инструментов сборки
# Устанавливаем все зависимости из Build-Depends в debian/control
RUN apt-get update && apt-get install -y \
    build-essential \
    devscripts \
    lintian \
    fakeroot \
    debhelper \
    default-jdk-headless \
    maven \
    python3 \
    zip \
    unzip \
    libzstd-dev \
    libsnappy-dev \
    zlib1g-dev \
    libssl-dev \
    nodejs \
    npm \
    bash \
    ca-certificates \
    curl \
    git \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем yarn глобально
RUN npm install -g yarn

# Обновление сертификатов
RUN update-ca-certificates

# Настройка окружения для debuild
# debuild требует, чтобы пользователь был в /etc/passwd
RUN useradd -m -s /bin/bash builder || true

# Выбор рабочей директории
WORKDIR /workspace

# Копируем исходники (вместе с debian/)
COPY ./apache-pulsar-4.1.2 /workspace/apache-pulsar-tmp

# Создаем ЧИСТЫЙ upstream-каталог (без debian/)
RUN cp -r apache-pulsar-tmp apache-pulsar-4.1.2 && \
    rm -rf apache-pulsar-4.1.2/debian

# Создаем правильный .orig.tar.gz
RUN tar -czf apache-pulsar_4.1.2.orig.tar.gz apache-pulsar-4.1.2

# Теперь добавляем debian/ в исходники перед сборкой
RUN cp -r apache-pulsar-tmp/debian apache-pulsar-4.1.2/

# Заходим в директорию исходников
WORKDIR /workspace/apache-pulsar-4.1.2

# Назначение корректных прав
RUN chmod 644 debian/* 2>/dev/null || true && \
    chmod 755 debian/rules && \
    chmod 755 debian/postinst debian/prerm 2>/dev/null || true

# Устанавливаем права на man страницы, если они есть
RUN if [ -d debian/man ]; then \
        chmod 644 debian/man/*.1 2>/dev/null || true; \
    fi

# Создание папки для собранного deb файла
RUN mkdir -p /mnt/deb

# Настройка переменных окружения для Maven
ENV MAVEN_OPTS="-Xmx5G"
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Переключаемся на пользователя builder (debuild предпочитает не root)
# Но для простоты тестирования оставляем root, так как в контейнере это безопасно
# Если нужна сборка от пользователя, раскомментируйте:
# USER builder

# Сборка пакета
# -us -uc означает: не подписывать (us=unsigned, uc=unsigned changes)
# -b означает: сборка только бинарного пакета (без исходников)
CMD debuild -us -uc -b && \
    cp /workspace/*.deb /mnt/deb/ 2>/dev/null || true && \
    cp /workspace/*.changes /mnt/deb/ 2>/dev/null || true && \
    cp /workspace/*.buildinfo /mnt/deb/ 2>/dev/null || true && \
    echo "Build complete! Files copied to /mnt/deb/" && \
    ls -lh /mnt/deb/ && \
    tail -f /dev/null
