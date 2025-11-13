# Используем официальный образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код приложения в контейнер
COPY . .

# Команда для запуска uvicorn
# Она указывает uvicorn импортировать 'app' из модуля 'main'
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# Если ваш main.py находится в подпапке, например app/main.py,
# используйте: CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]