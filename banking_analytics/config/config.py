# config/config.py
import os
from pathlib import Path


class Config:
    """Базовый класс конфигурации"""

    # Настройки проекта
    PROJECT_NAME = "Banking Analytics Pipeline"
    VERSION = "1.0.0"

    # Пути
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"

    DATA_DIR.mkdir(exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)

    # PostgreSQL настройки
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'trst_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '123')

    # Генератор данных
    NUM_CUSTOMERS = int(os.getenv('NUM_CUSTOMERS', 1000))
    NUM_TRANSACTIONS = int(os.getenv('NUM_TRANSACTIONS', 10000))
    NUM_BRANCHES = int(os.getenv('NUM_BRANCHES', 50))

    # API
    CURRENCY_API_URL = "https://www.cbr-xml-daily.ru/daily_json.js"
    API_TIMEOUT = 10

    # Схемы БД
    STAGING_SCHEMA = "staging"
    DWH_SCHEMA = "dwh"


class DevelopmentConfig(Config):
    """Конфигурация для разработки"""
    DEBUG = True
    NUM_CUSTOMERS = 100
    NUM_TRANSACTIONS = 1000


config_by_name = {
    'dev': DevelopmentConfig,
    'development': DevelopmentConfig
}


def get_config(env=None):
    if env is None:
        env = os.getenv('ENV', 'dev')
    return config_by_name.get(env.lower(), DevelopmentConfig)


current_config = get_config()
