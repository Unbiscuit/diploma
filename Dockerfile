FROM python:3.13-slim

# Указываем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем зависимости и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект внутрь контейнера
COPY . .

# Указываем команду запуска
CMD ["python", "-m", "app.main"]