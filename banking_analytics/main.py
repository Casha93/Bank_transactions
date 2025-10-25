# main.py
from config.config import get_config
from data_generator.fake_data_generator import BankingDataGenerator
from api.currency_api import CurrencyAPI
from database.db_connection import DatabaseConnection
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader
import os


def main():
    # Получаем конфигурацию (можно задать через export ENV=prod)
    config = get_config()

    print(f"=== {config.PROJECT_NAME} v{config.VERSION} ===")
    print(f"Окружение: {os.getenv('ENV', 'development')}\n")

    # 1. Генерация данных
    print("1. Генерация данных...")
    generator = BankingDataGenerator(
        num_customers=config.NUM_CUSTOMERS,
        num_transactions=config.NUM_TRANSACTIONS
    )
    customers_df = generator.generate_customers()
    accounts_df = generator.generate_accounts(len(customers_df))
    transactions_df = generator.generate_transactions(accounts_df)
    branches_df = generator.generate_branches()

    # 2. Получение курсов валют через API
    print("\n2. Получение курсов валют...")
    currency_api = CurrencyAPI()
    exchange_rates_df = currency_api.get_exchange_rates()

    # 3. Подключение к PostgreSQL
    print("\n3. Подключение к PostgreSQL...")
    db = DatabaseConnection(
        host=config.DB_HOST,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        port=config.DB_PORT
    )
    db.connect()

    # 4. Загрузка в staging
    print("\n4. Загрузка в staging...")
    db.load_dataframe(customers_df, 'customers', schema=config.STAGING_SCHEMA)
    db.load_dataframe(accounts_df, 'accounts', schema=config.STAGING_SCHEMA)
    db.load_dataframe(transactions_df, 'transactions', schema=config.STAGING_SCHEMA)
    db.load_dataframe(branches_df, 'branches', schema=config.STAGING_SCHEMA)
    db.load_dataframe(exchange_rates_df, 'exchange_rates', schema=config.STAGING_SCHEMA)

    # 5. Извлечение данных из staging (НОВОЕ!)
    print("\n5. Извлечение данных из staging...")
    extractor = DataExtractor(db)
    staging_data = extractor.extract_all_staging_data()

    # 6. Обработка и обогащение
    print("\n6. Обработка и обогащение данных...")
    transformer = DataTransformer(staging_data['exchange_rates'])

    customers_clean = transformer.clean_customers(staging_data['customers'])
    transactions_clean = transformer.clean_transactions(staging_data['transactions'])
    transactions_enriched = transformer.enrich_with_currency_rates(transactions_clean)

    date_dim_df = transformer.create_date_dimension('2023-01-01', '2025-12-31')

    # 7. Загрузка в DWH
    print("\n7. Загрузка в схему звезда...")
    loader = DataLoader(db)
    loader.load_dimensions(customers_clean, staging_data['accounts'],
                           staging_data['branches'], date_dim_df)
    loader.load_fact_table(transactions_enriched)

    # 8. Проверка
    print("\n8. Проверка результатов...")
    query = """
    SELECT 
        COUNT(*) as total_transactions,
        SUM(amount_rub) as total_amount_rub,
        AVG(amount_rub) as avg_amount_rub
    FROM dwh.fact_transactions
    """
    result = db.read_query(query)
    print(result)

    db.close()
    print("\n=== Pipeline выполнен успешно! ===")


if __name__ == "__main__":
    main()
