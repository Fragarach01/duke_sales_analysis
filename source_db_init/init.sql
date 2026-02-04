CREATE TABLE IF NOT EXISTS transactions (
    date VARCHAR(50) NOT NULL,
    time VARCHAR(50) NOT NULL,
    order_no VARCHAR(50) NOT NULL,
    account_no VARCHAR(50) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    terminal VARCHAR(50) NOT NULL,
    type VARCHAR(50) NOT NULL,
    description VARCHAR(100) NOT NULL,
    destination VARCHAR(50),
    detail VARCHAR(50),
    information VARCHAR(50),
    employee VARCHAR(50),
    manager_override VARCHAR(50),
    table_no VARCHAR(50),
    customer_name VARCHAR(50),
    price_override VARCHAR(50),
    covers VARCHAR(50),
    quantity VARCHAR(50),
    sales_amount VARCHAR(50),
    discount VARCHAR(50),
    service_charge VARCHAR(50),
    tip VARCHAR(50),
    payment_amount VARCHAR(50),
    forfeit_amount VARCHAR(50),
    VAT VARCHAR(50),
    other_tax VARCHAR(50),
    cashback VARCHAR(50),
    change VARCHAR(50),
    tmp VARCHAR(50) -- accounts for extra column created due to trailing comma in CSV
);

CREATE TABLE IF NOT EXISTS loaded_files (filename TEXT PRIMARY KEY);
--COPY transactions FROM '/sales_data/transactions_11-24.csv' PROGRAM 'cat *.csv' WITH (FORMAT csv, HEADER true, LOG_VERBOSITY verbose);

