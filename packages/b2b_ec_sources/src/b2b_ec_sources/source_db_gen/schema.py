"""Schema SQL constants for the relational source generator."""

CORE_SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS ref_countries (code TEXT PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS companies (
        cuit TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT CHECK (type IN ('Supplier', 'Client')),
        country_code TEXT REFERENCES ref_countries(code),
        created_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
        updated_at TIMESTAMP DEFAULT (now() at time zone 'utc')
    );
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        supplier_cuit TEXT REFERENCES companies(cuit),
        base_price DECIMAL(12,2),
        created_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
        updated_at TIMESTAMP DEFAULT (now() at time zone 'utc')
    );
    CREATE TABLE IF NOT EXISTS company_catalogs (
        company_cuit TEXT REFERENCES companies(cuit),
        product_id INTEGER REFERENCES products(id),
        sale_price DECIMAL(12,2),
        created_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
        updated_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
        PRIMARY KEY (company_cuit, product_id)
    );
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        company_cuit TEXT REFERENCES companies(cuit),
        document_number TEXT UNIQUE NOT NULL,
        username TEXT UNIQUE NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone_number TEXT,
        birth_date DATE NOT NULL,
        created_at TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        company_cuit TEXT REFERENCES companies(cuit),
        customer_id INTEGER REFERENCES customers(id),
        status TEXT CHECK (status IN ('COMPLETED', 'CANCELLED', 'RETURNED')),
        order_date TIMESTAMP,
        total_amount DECIMAL(15,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP DEFAULT (now() at time zone 'utc')
    );
    CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id),
        product_id INTEGER,
        quantity INTEGER,
        unit_price DECIMAL(12,2)
    );
"""

LEAD_CONVERSIONS_SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS lead_conversions (
        lead_id TEXT PRIMARY KEY,
        company_cuit TEXT NOT NULL REFERENCES companies(cuit),
        company_name TEXT NOT NULL,
        country_code TEXT REFERENCES ref_countries(code),
        lead_status TEXT,
        lead_source TEXT,
        conversion_probability DOUBLE PRECISION,
        converted_at TIMESTAMP NOT NULL,
        source_file TEXT,
        conversion_window_days INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT (now() at time zone 'utc')
    );
    CREATE INDEX IF NOT EXISTS idx_lead_conversions_company_cuit
        ON lead_conversions(company_cuit);
    CREATE INDEX IF NOT EXISTS idx_lead_conversions_converted_at
        ON lead_conversions(converted_at);
"""

