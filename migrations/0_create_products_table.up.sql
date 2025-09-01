BEGIN;

CREATE TABLE IF NOT EXISTS products (
    id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    product_id varchar(100) NOT NULL,
    name varchar(100) NOT NULL,
    sku varchar(100) NOT NULL,
    brand varchar(100) NOT NULL,
    category varchar(100) NOT NULL,
    description text NOT NULL,
    price_amount DECIMAL(18,2) NOT NULL,
    price_currency varchar(30) NOT NULL,
    available_stock int NOT NULL,
    tags varchar(30)[] NOT NULL,
    images json NOT NULL,
    specifications json NOT NULL,
    store_id varchar(100) NOT NULL,
    PRIMARY KEY (product_id, store_id)
);

CREATE INDEX IF NOT EXISTS products_name_index ON products (name);

COMMIT;
