-- Minimal e-commerce schema for r2g SQL Server integration tests.
--
-- Mirrors docker/mysql_demo/schema.sql: single + composite PKs, a single FK
-- that becomes an edge (orders -> customers), and a two-FK join table
-- (order_items -> orders, products). Uses a spread of SQL Server types,
-- including BIT (which the connector maps to boolean).
--
-- T-SQL, no `GO` batch separators (executed as one batch by pymssql), and
-- idempotent: existing tables are dropped first so row counts are
-- deterministic across re-seeds. The SQL Server image does not auto-run
-- init scripts, so the integration test executes this file itself.

DROP TABLE IF EXISTS dbo.order_items;
DROP TABLE IF EXISTS dbo.orders;
DROP TABLE IF EXISTS dbo.products;
DROP TABLE IF EXISTS dbo.customers;

CREATE TABLE dbo.customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    name        NVARCHAR(100) NOT NULL,
    email       NVARCHAR(200) NULL,
    is_active   BIT NOT NULL DEFAULT 1
);

CREATE TABLE dbo.products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    title      NVARCHAR(200) NOT NULL,
    price      DECIMAL(10, 2) NOT NULL
);

CREATE TABLE dbo.orders (
    order_id    INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    created_at  DATETIME2 NOT NULL,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES dbo.customers (customer_id)
);

CREATE TABLE dbo.order_items (
    order_id   INT NOT NULL,
    product_id INT NOT NULL,
    quantity   INT NOT NULL,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_oi_order
        FOREIGN KEY (order_id) REFERENCES dbo.orders (order_id),
    CONSTRAINT fk_oi_product
        FOREIGN KEY (product_id) REFERENCES dbo.products (product_id)
);

INSERT INTO dbo.customers (name, email, is_active) VALUES
    ('Alice', 'alice@example.com', 1),
    ('Bob',   'bob@example.com',   1),
    ('Carol', NULL,                0);

INSERT INTO dbo.products (title, price) VALUES
    ('Widget', 9.99),
    ('Gadget', 19.95),
    ('Gizmo',  4.50);

INSERT INTO dbo.orders (customer_id, created_at) VALUES
    (1, '2026-01-02T10:00:00'),
    (1, '2026-01-05T14:30:00'),
    (2, '2026-02-11T09:15:00');

INSERT INTO dbo.order_items (order_id, product_id, quantity) VALUES
    (1, 1, 2),
    (1, 2, 1),
    (2, 3, 5),
    (3, 1, 1);
