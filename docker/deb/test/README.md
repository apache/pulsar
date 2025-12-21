# Тестирование deb пакета Apache Pulsar

## Использование

1. Соберите deb пакет (используя `docker/deb/build/Dockerfile`)

2. Скопируйте deb пакет в эту директорию:
   
   cp /path/to/apache-pulsar_*.deb docker/deb/test/
   3. Соберите тестовый образ:
   cd docker/deb/test
   docker build -t apache-pulsar-test .
   4. Запустите тесты:
  
   docker run --rm --privileged apache-pulsar-test
   
   Или с монтированием deb файла:
  
   docker run --rm --privileged -v /path/to/deb:/test:ro apache-pulsar-test
   ## Что проверяется

- Установка пакета
- Наличие всех файлов и директорий
- Наличие systemd сервисов
- Создание системного пользователя
- Права доступа
- Исполняемость скриптов
- Работа команд (базовая проверка)

## Примечания

- Для полного тестирования systemd требуется запуск с `--privileged`
- Для запуска сервисов нужны ZooKeeper и BookKeeper (не включено в базовый тест)
- Тест проверяет структуру пакета, но не запускает сервисы