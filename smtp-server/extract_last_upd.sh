#!/bin/bash

# Параметры подключения
USER="your_username"             # Ваш логин
HOST="ftp.server.com"            # Адрес вашего FTP-сервера
REMOTE_DIR="dirs"                # Директория на сервере
LOCAL_DIR="/path/to/local/dir"   # Локальная директория для сохранения файла

# Проверка существования локальной директории
if [ ! -d "$LOCAL_DIR" ]; then
    mkdir -p "$LOCAL_DIR"
fi

# Находим последний измененный файл на удаленном сервере
LAST_FILE=$(ssh "$USER@$HOST" "ls -t $REMOTE_DIR | head -n 1")

# Проверка существования файла
if [ -z "$LAST_FILE" ]; then
    echo "Не удалось найти файлы в директории $REMOTE_DIR"
    exit 1
fi

# Копируем последний файл с сервера
scp "$USER@$HOST:$REMOTE_DIR/$LAST_FILE" "$LOCAL_DIR"

# Проверка успешности выполнения команды
# Проверка успешности выполнения команды
if [ $? -eq 0 ]; then
    echo "Файл '$LAST_FILE' успешно скопирован в $LOCAL_DIR"
else
    echo "Ошибка при копировании файла"
fi
