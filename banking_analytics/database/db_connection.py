# database/db_connection.py
"""
Модуль для управления подключением к PostgreSQL
"""
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


class DatabaseConnection:
    """Управление подключением к PostgreSQL"""

    def __init__(self, host='localhost', database='trst_db', user='postgres',
                 password='123', port=5432):
        """
        Инициализация параметров подключения

        Args:
            host: адрес сервера (по умолчанию localhost)
            database: название базы данных
            user: имя пользователя
            password: пароль
            port: порт PostgreSQL (по умолчанию 5432)
        """
        self.conn_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.conn = None

    def connect(self):
        """Установка соединения с базой данных"""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            print(f"✓ Подключение к PostgreSQL успешно установлено")
            print(f"  База данных: {self.conn_params['database']}")
            print(f"  Пользователь: {self.conn_params['user']}")
            print(f"  Хост: {self.conn_params['host']}:{self.conn_params['port']}")

            # Проверка версии PostgreSQL
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"  Версия: {version.split(',')[0]}")

        except psycopg2.OperationalError as e:
            print(f"✗ Ошибка подключения к базе данных:")
            print(f"  {e}")
            print("\nВозможные причины:")
            print("  1. PostgreSQL не запущен")
            print("  2. Неверные учетные данные")
            print("  3. База данных не существует")
            raise
        except Exception as e:
            print(f"✗ Неожиданная ошибка: {e}")
            raise

    def execute_query(self, query, params=None):
        """
        Выполнение SQL запроса

        Args:
            query: SQL запрос
            params: параметры запроса (optional)
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Ошибка выполнения запроса: {e}")
            raise

    def load_dataframe(self, df, table_name, schema='staging'):
        """
        Загрузка DataFrame в PostgreSQL

        Args:
            df: pandas DataFrame для загрузки
            table_name: название таблицы
            schema: схема базы данных (по умолчанию 'staging')
        """
        if df.empty:
            print(f"⚠ DataFrame пустой, пропуск загрузки в {schema}.{table_name}")
            return

        try:
            columns = ', '.join(df.columns)
            values = [tuple(x) for x in df.values]

            query = f"""
            INSERT INTO {schema}.{table_name} ({columns})
            VALUES %s
            ON CONFLICT DO NOTHING
            """

            with self.conn.cursor() as cursor:
                execute_values(cursor, query, values)
                self.conn.commit()

            print(f"✓ Загружено {len(df)} записей в {schema}.{table_name}")

        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"✗ Ошибка загрузки в {schema}.{table_name}: {e}")
            raise

    def read_query(self, query):
        """
        Чтение данных из базы с помощью SQL запроса

        Args:
            query: SQL запрос SELECT

        Returns:
            pandas DataFrame с результатами
        """
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Ошибка выполнения запроса: {e}")
            raise

    def test_connection(self):
        """Проверка подключения к базе данных"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                result = cursor.fetchone()
                if result[0] == 1:
                    print("✓ Тест подключения успешен")
                    return True
        except Exception as e:
            print(f"✗ Тест подключения провален: {e}")
            return False

    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.conn.close()
            print("✓ Соединение с базой данных закрыто")


# Функция для быстрого тестирования подключения
def test_db_connection():
    """Тестовая функция для проверки подключения"""
    print("=== Тест подключения к PostgreSQL ===\n")

    # Создаем подключение с вашими данными
    db = DatabaseConnection(
        host='localhost',
        database='trst_db',
        user='postgres',
        password='123',
        port=5432
    )

    try:
        # Подключаемся
        db.connect()

        # Тестируем подключение
        db.test_connection()

        # Проверяем существующие схемы
        print("\nПроверка существующих схем:")
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schema_name;
        """
        schemas = db.read_query(query)
        print(schemas)

        # Закрываем соединение
        db.close()

        print("\n=== Тест завершен успешно ===")

    except Exception as e:
        print(f"\n=== Тест завершен с ошибкой ===")
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    # Запускаем тест при прямом запуске файла
    test_db_connection()
