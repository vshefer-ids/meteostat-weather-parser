"""Модуль конфигурации и инициализации.

Отвечает за загрузку, валидацию и предоставление всех настроек приложения
из переменных окружения.

Ключевые задачи:
1. Загрузка переменных из файла .env для локальной разработки.
2. Валидация наличия и корректности формата критически важных переменных.
   Приложение завершит работу с ошибкой, если конфигурация неполная или неверна.
3. Настройка глобальной конфигурации логирования для всего проекта.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# 1. Загружаем переменные из файла .env (если он есть)
load_dotenv()

# 2. Настраиваем логирование для вывода в консоль (идеально для Docker)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 3. Валидация: проверяем наличие всех обязательных переменных
REQUIRED_VARS = [
    "LAT", "LON", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_TABLE"
]
missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]

if missing_vars:
    # Если чего-то не хватает, логируем критическую ошибку и выходим
    error_message = f"Критические переменные окружения не установлены: {', '.join(missing_vars)}"
    logging.critical(error_message)
    sys.exit(f"Ошибка конфигурации: {error_message}")


# 4. Загружаем и преобразуем переменные с обработкой ошибок
try:
    # --- Параметры геолокации и парсинга ---
    LOCATION_NAME = os.getenv("LOCATION_NAME", "Default Location")
    LAT = float(os.getenv("LAT"))
    LON = float(os.getenv("LON"))
    DAYS_BACK = int(os.getenv("DAYS_BACK", 30))
    DELAY_MINUTES = int(os.getenv("DELAY_MINUTES", 65))

    # --- Параметры базы данных ---
    POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
    POSTGRES_CONN = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES_PORT")),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    logging.info("Конфигурация успешно загружена и валидирована.")

except (ValueError, TypeError) as e:
    # Если ошибка при преобразовании (например, float("abc")), логируем и выходим
    error_message = f"Ошибка в формате одной из переменных окружения (например, неверное число): {e}"
    logging.critical(error_message)
    sys.exit(f"Ошибка конфигурации: {error_message}")