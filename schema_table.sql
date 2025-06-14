CREATE TYPE gender_type AS ENUM ('male', 'female');

CREATE TABLE IF NOT EXISTS mall_customers_clean (
    customer_id INT PRIMARY KEY,
    gender gender_type,
    age INT NOT NULL,
    annual_income INT NOT NULL,
    spending_score INT NOT NULL
);

CREATE TABLE IF NOT EXISTS quotes (
    id VARCHAR(16) PRIMARY KEY,
    quote TEXT NOT NULL,
    author VARCHAR(255),
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON quotes
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_updated_at_products
BEFORE UPDATE ON mall_customers_clean
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
