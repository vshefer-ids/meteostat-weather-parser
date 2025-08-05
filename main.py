"""Основной исполняемый модуль сервиса сбора погодных данных.

Этот скрипт запускает бесконечный цикл, который периодически выполняет
основную логику парсера: определяет необходимый временной диапазон,
запрашивает данные у сервиса Meteostat и сохраняет их в базу данных.
"""

import logging
import time
from datetime import datetime, timedelta

from meteostat import Point, Hourly

# Импортируем все настройки и функции из наших модулей
from config import LOCATION_NAME, LAT, LON, DAYS_BACK, DELAY_MINUTES
from db_writer import get_last_timestamp, write_data


def run_parser() -> None:
    """Выполняет один полный цикл сбора и сохранения данных о погоде.

    Определяет, с какой даты начинать сбор: либо со следующего часа
    после последней сохраненной в БД записи, либо за последние N дней
    (если БД пуста). Запрашивает почасовые данные через библиотеку meteostat
    и, если данные получены, передает их для пакетной записи в БД.
    """
    point = Point(LAT, LON)
    last_timestamp = get_last_timestamp()

    if last_timestamp:
        # Начинаем сбор со следующего часа после последней записи
        start_date = last_timestamp + timedelta(hours=1)
    else:
        # Если база пуста, берем данные за последние DAYS_BACK дней
        start_date = datetime.now() - timedelta(days=DAYS_BACK)

    end_date = datetime.now()

    # Проверка: если данные уже актуальны, не делаем лишних запросов
    if start_date >= end_date:
        logging.info(f"Данные для '{LOCATION_NAME}' полностью актуальны. Новых данных нет.")
        return

    logging.info(
        f"Запрашиваем данные для '{LOCATION_NAME}' "
        f"с {start_date:%Y-%m-%d %H:%M} по {end_date:%Y-%m-%d %H:%M}"
    )

    try:
        # Получаем данные из API
        data_source = Hourly(point, start_date, end_date)
        df = data_source.fetch()

        if not df.empty:
            logging.info(f"Получено {len(df)} новых записей для '{LOCATION_NAME}'.")
            write_data(df)
        else:
            logging.info(f"API Meteostat не вернуло новых данных для '{LOCATION_NAME}' за указанный период.")

    except Exception as e:
        # Ловим возможные ошибки от API meteostat (например, недоступность)
        logging.error(f"Ошибка при получении данных от Meteostat: {e}")


if __name__ == "__main__":
    """Точка входа в приложение. Запускает бесконечный цикл опроса."""
    logging.info("Сервис сбора погодных данных запущен.")
    while True:
        try:
            run_parser()
        except Exception as e:
            # Ловим любые непредвиденные ошибки в главном цикле, чтобы сервис не упал
            logging.critical(f"Произошла критическая ошибка в главном цикле: {e}", exc_info=True)
        
        logging.info(f"Следующий запуск через {DELAY_MINUTES} минут...\n")
        time.sleep(DELAY_MINUTES * 60)