# create_schemas.py
"""
Скрипт для создания схем и таблиц в базе данных
"""
from database.db_connection import DatabaseConnection


def create_schemas_and_tables():
    """Создание схем staging и dwh с таблицами"""

    print("=== Создание схем и таблиц в базе данных ===\n")

    db = DatabaseConnection(
        host='localhost',
        database='trst_db',
        user='postgres',
        password='123'
    )

    try:
        db.connect()

        # SQL команды для создания всех схем и таблиц
        sql_commands = """
        -- Создание staging схемы
        CREATE SCHEMA IF NOT EXISTS staging;

        -- Создание dwh схемы
        CREATE SCHEMA IF NOT EXISTS dwh;

        -- Staging таблицы
        CREATE TABLE IF NOT EXISTS staging.customers (
            customer_id INTEGER PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            phone VARCHAR(50),
            date_of_birth DATE,
            city VARCHAR(100),
            country VARCHAR(100),
            registration_date DATE,
            customer_segment VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS staging.accounts (
            account_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            account_number VARCHAR(50),
            account_type VARCHAR(50),
            currency VARCHAR(10),
            balance DECIMAL(15, 2),
            opening_date DATE,
            status VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS staging.transactions (
            transaction_id INTEGER PRIMARY KEY,
            account_id INTEGER,
            transaction_date TIMESTAMP,
            transaction_type VARCHAR(50),
            amount DECIMAL(15, 2),
            currency VARCHAR(10),
            merchant_name VARCHAR(200),
            transaction_status VARCHAR(50),
            channel VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS staging.branches (
            branch_id INTEGER PRIMARY KEY,
            branch_name VARCHAR(100),
            city VARCHAR(100),
            address TEXT,
            region VARCHAR(50),
            opening_date DATE
        );

        CREATE TABLE IF NOT EXISTS staging.exchange_rates (
            date DATE PRIMARY KEY,
            usd_to_rub DECIMAL(10, 4),
            eur_to_rub DECIMAL(10, 4),
            usd_to_eur DECIMAL(10, 4)
        );

        -- DWH Dimension Tables
        CREATE TABLE IF NOT EXISTS dwh.dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_id INTEGER,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            full_name VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            age INTEGER,
            city VARCHAR(100),
            country VARCHAR(100),
            customer_segment VARCHAR(50),
            registration_date DATE,
            effective_date DATE,
            expiration_date DATE,
            is_current BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_account (
            account_key SERIAL PRIMARY KEY,
            account_id INTEGER,
            account_number VARCHAR(50),
            account_type VARCHAR(50),
            currency VARCHAR(10),
            opening_date DATE,
            status VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_date (
            date_key INTEGER PRIMARY KEY,
            date DATE,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name VARCHAR(20),
            week INTEGER,
            day_of_month INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            is_weekend BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_transaction_type (
            transaction_type_key SERIAL PRIMARY KEY,
            transaction_type VARCHAR(50),
            transaction_category VARCHAR(50),
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_branch (
            branch_key SERIAL PRIMARY KEY,
            branch_id INTEGER,
            branch_name VARCHAR(100),
            city VARCHAR(100),
            region VARCHAR(50),
            address TEXT
        );

        -- DWH Fact Table
        CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
            transaction_key SERIAL PRIMARY KEY,
            transaction_id INTEGER,
            date_key INTEGER,
            customer_key INTEGER,
            account_key INTEGER,
            transaction_type_key INTEGER,
            branch_key INTEGER,
            amount_original DECIMAL(15, 2),
            original_currency VARCHAR(10),
            amount_rub DECIMAL(15, 2),
            exchange_rate DECIMAL(10, 4),
            transaction_status VARCHAR(50),
            channel VARCHAR(50),
            merchant_name VARCHAR(200)
        );

        -- Создание индексов для оптимизации запросов
        CREATE INDEX IF NOT EXISTS idx_fact_date ON dwh.fact_transactions(date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_customer ON dwh.fact_transactions(customer_key);
        CREATE INDEX IF NOT EXISTS idx_fact_account ON dwh.fact_transactions(account_key);
        """

        print("Выполнение SQL команд...")
        db.execute_query(sql_commands)

        print("\n✓ Все схемы и таблицы успешно созданы!\n")

        # Проверка созданных таблиц
        print("Проверка созданных таблиц в staging:")
        query_staging = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'staging'
        ORDER BY table_name;
        """
        staging_tables = db.read_query(query_staging)
        print(staging_tables)

        print("\nПроверка созданных таблиц в dwh:")
        query_dwh = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'dwh'
        ORDER BY table_name;
        """
        dwh_tables = db.read_query(query_dwh)
        print(dwh_tables)

        db.close()
        print("\n=== Создание завершено успешно ===")

    except Exception as e:
        print(f"\n✗ Ошибка при создании схем: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    create_schemas_and_tables()
