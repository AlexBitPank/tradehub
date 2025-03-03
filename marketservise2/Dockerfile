FROM python:3.12.5-alpine

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект в контейнер
COPY . .

# Открываем порт (если бот использует вебхуки или веб-сервер, например)
# EXPOSE 8000  # Откройте порт, если нужно

# Команда для запуска бота
CMD ["python", "app2.py"]
