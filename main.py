"""Основной исполняемый модуль сервиса сбора погодных данных."""

import logging
import time
from datetime import datetime, timedelta, timezone
import pytz

from meteostat import Point, Hourly

from config import LOCATION_NAME, LAT, LON, DAYS_BACK, DELAY_MINUTES
from db_writer import get_last_timestamp, write_data

DB_TIMEZONE = pytz.timezone('Europe/Moscow')


def run_parser() -> None:
    """
    Выполняет один цикл сбора данных, корректно обрабатывая часовые пояса.
    """
    point = Point(LAT, LON)
    last_timestamp_naive = get_last_timestamp()
    end_date_utc = datetime.now(timezone.utc)

    if last_timestamp_naive:
        last_timestamp_aware = DB_TIMEZONE.localize(last_timestamp_naive)
        start_date_utc = last_timestamp_aware.astimezone(timezone.utc) + timedelta(hours=1)
    else:
        start_date_utc = end_date_utc - timedelta(days=DAYS_BACK)

    if start_date_utc >= end_date_utc:
        logging.info(f"Данные для '{LOCATION_NAME}' полностью актуальны. Новых данных нет.")
        return

    logging.info(
        f"Запрашиваем данные для '{LOCATION_NAME}' "
        f"с {start_date_utc:%Y-%m-%d %H:%M %Z} по {end_date_utc:%Y-%m-%d %H:%M %Z}"
    )

    try:
        # --- ФИНАЛЬНЫЙ ФИКС ---
        # Библиотека meteostat ожидает НАИВНЫЕ datetime объекты, но в UTC.
        # Наша логика уже привела их к UTC, осталось только убрать метку tzinfo.
        start_date_for_api = start_date_utc.replace(tzinfo=None)
        end_date_for_api = end_date_utc.replace(tzinfo=None)

        # Передаем в библиотеку наивные UTC-даты
        data_source = Hourly(point, start_date_for_api, end_date_for_api)
        df = data_source.fetch()

        if not df.empty:
            logging.info(f"Получено {len(df)} новых записей для '{LOCATION_NAME}'.")
            write_data(df)
        else:
            logging.info(f"API Meteostat не вернуло новых данных для '{LOCATION_NAME}' за указанный период.")

    except Exception as e:
        logging.error(f"Ошибка при получении данных от Meteostat: {e}")


if __name__ == "__main__":
    logging.info("Сервис сбора погодных данных запущен.")
    while True:
        try:
            run_parser()
        except Exception as e:
            logging.critical(f"Произошла критическая ошибка в главном цикле: {e}", exc_info=True)
        
        logging.info(f"Следующий запуск через {DELAY_MINUTES} минут...\n")
        time.sleep(DELAY_MINUTES * 60)