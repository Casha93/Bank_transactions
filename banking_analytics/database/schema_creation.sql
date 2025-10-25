-- Создание staging схемы для сырых данных
CREATE SCHEMA IF NOT EXISTS staging;

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

-- Создание DWH схемы для схемы звезда
CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimension Tables (измерения)
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
    -- SCD Type 2 поля
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

-- Fact Table (факты)
CREATE TABLE IF NOT EXISTS dwh.fact_transactions (
    transaction_key SERIAL PRIMARY KEY,
    transaction_id INTEGER,
    date_key INTEGER REFERENCES dwh.dim_date(date_key),
    customer_key INTEGER REFERENCES dwh.dim_customer(customer_key),
    account_key INTEGER REFERENCES dwh.dim_account(account_key),
    transaction_type_key INTEGER REFERENCES dwh.dim_transaction_type(transaction_type_key),
    branch_key INTEGER REFERENCES dwh.dim_branch(branch_key),
    -- Метрики (меры)
    amount_original DECIMAL(15, 2),
    original_currency VARCHAR(10),
    amount_rub DECIMAL(15, 2),  -- Все суммы конвертированы в рубли
    exchange_rate DECIMAL(10, 4),
    transaction_status VARCHAR(50),
    channel VARCHAR(50),
    merchant_name VARCHAR(200)
);

-- Индексы для оптимизации
CREATE INDEX idx_fact_date ON dwh.fact_transactions(date_key);
CREATE INDEX idx_fact_customer ON dwh.fact_transactions(customer_key);
CREATE INDEX idx_fact_account ON dwh.fact_transactions(account_key);
