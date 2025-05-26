FROM ubuntu:24.04
# Обновление системы и установка необходимых пакетов
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libglib2.0-dev \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    cron \
    python3 \
    python3-pip \
    python-is-python3 \
    net-tools \
    nano \
    rsyslog \
    && rm -rf /var/lib/apt/lists/*
# Устанавливаем редактор по умолчанию
ENV EDITOR=/bin/nano
# Копируем CRM в рабочую директорию
COPY ./app /root/app
COPY ./.env /root/app/.env
WORKDIR /root/app
# Устанавливаем зависимости Python
RUN pip install -r requirements.txt --break-system-packages
# Устанавливаем точку входа для запуска cron и SSH
