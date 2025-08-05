"""Модуль для взаимодействия с базой данных PostgreSQL.

Содержит функции для получения последней временной метки и для
высокопроизводительной пакетной записи данных о погоде.
"""

import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import AsIs
from pandas import DataFrame

from config import POSTGRES_CONN, POSTGRES_TABLE


def get_last_timestamp() -> datetime | None:
    """Получает временную метку последней записи из БД.

    Устанавливает соединение с PostgreSQL и выполняет запрос на получение
    максимального значения из столбца 'time'.

    Returns:
        Объект datetime с временной меткой последней записи, если она есть,
        в противном случае None.
    """
    try:
        with psycopg2.connect(**POSTGRES_CONN) as conn:
            with conn.cursor() as cur:
                # Используем AsIs для безопасной подстановки имени таблицы
                cur.execute("SELECT MAX(time) FROM %s", (AsIs(POSTGRES_TABLE),))
                result = cur.fetchone()

                if result and result[0]:
                    logging.info(f"Последняя запись в БД найдена: {result[0]}")
                    return result[0]
                
                logging.info("База данных пуста. Будет выполнена полная загрузка.")
                return None
                
    except psycopg2.Error as e:
        logging.error(f"Ошибка при получении последней даты из БД: {e}")
        return None


def write_data(df: DataFrame) -> None:
    """Записывает данные из DataFrame в PostgreSQL методом пакетной вставки.

    Эффективно вставляет или обновляет множество записей за один запрос,
    используя `execute_values`. Применяет конструкцию `ON CONFLICT DO UPDATE`
    для реализации логики "UPSERT" (обновить, если существует, иначе вставить).

    Args:
        df: Pandas DataFrame с данными о погоде. Ожидается, что индекс
            содержит время, а колонка 'temp' — температуру.
    """
    # 1. Подготовка данных: отбрасываем строки без температуры и создаем список кортежей
    data_to_insert = [
        (index.to_pydatetime(), row['temp'])
        for index, row in df.dropna(subset=['temp']).iterrows()
    ]

    if not data_to_insert:
        logging.warning("Нет валидных данных для записи после фильтрации.")
        return

    # 2. Пакетная вставка/обновление в БД
    try:
        with psycopg2.connect(**POSTGRES_CONN) as conn:
            with conn.cursor() as cur:
                # Шаблон SQL-запроса для массовой вставки/обновления
                query_template = f"""
                    INSERT INTO {POSTGRES_TABLE} (time, temperature)
                    VALUES %s
                    ON CONFLICT (time) DO UPDATE SET temperature = EXCLUDED.temperature;
                """
                
                # Выполнение пакетной операции - САМАЯ ГЛАВНАЯ ЧАСТЬ
                execute_values(cur, query_template, data_to_insert)
                conn.commit()
                logging.info(f"Успешно записано/обновлено {len(data_to_insert)} строк в БД.")

    except psycopg2.Error as e:
        logging.error(f"Ошибка при пакетной записи в БД: {e}")