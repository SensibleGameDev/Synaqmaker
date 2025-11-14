# Используем официальный образ с компилятором C++ (g++)
FROM gcc:latest
RUN apt-get update && apt-get install -y python3 && rm -rf /var/lib/apt/lists/*
# Создаем такого же не-рут пользователя 'appuser'

RUN useradd -m -u 1000 appuser
USER appuser
WORKDIR /home/appuser