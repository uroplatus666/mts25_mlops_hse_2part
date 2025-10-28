FROM python:3.12-slim

WORKDIR /app

# Логи
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs # Права на запись для всех пользователей

# Matplotlib без дисплея
ENV MPLBACKEND=Agg

# Зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Точки монтирования
VOLUME /app/input
VOLUME /app/output
VOLUME /app/models

CMD ["python", "./app/app.py"]
