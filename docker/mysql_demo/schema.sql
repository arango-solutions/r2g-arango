-- Minimal e-commerce schema for r2g MySQL integration tests.
--
-- Exercises every introspection path the connector cares about:
--   * single-column PKs (customers, products, orders)
--   * a composite PK (order_items)
--   * a single-column FK that becomes an edge (orders -> customers)
--   * a two-FK join table that becomes edges (order_items -> orders, products)
--   * a spread of MySQL column types (int, varchar, decimal, datetime, tinyint)
--
-- InnoDB so the foreign keys are declared in information_schema.

CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(200),
    is_active   TINYINT NOT NULL DEFAULT 1
) ENGINE=InnoDB;

CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    title      VARCHAR(200) NOT NULL,
    price      DECIMAL(10, 2) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE orders (
    order_id    INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    created_at  DATETIME NOT NULL,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
) ENGINE=InnoDB;

CREATE TABLE order_items (
    order_id   INT NOT NULL,
    product_id INT NOT NULL,
    quantity   INT NOT NULL,
    PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_oi_order
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
    CONSTRAINT fk_oi_product
        FOREIGN KEY (product_id) REFERENCES products (product_id)
) ENGINE=InnoDB;

INSERT INTO customers (name, email, is_active) VALUES
    ('Alice', 'alice@example.com', 1),
    ('Bob',   'bob@example.com',   1),
    ('Carol', NULL,                0);

INSERT INTO products (title, price) VALUES
    ('Widget', 9.99),
    ('Gadget', 19.95),
    ('Gizmo',  4.50);

INSERT INTO orders (customer_id, created_at) VALUES
    (1, '2026-01-02 10:00:00'),
    (1, '2026-01-05 14:30:00'),
    (2, '2026-02-11 09:15:00');

INSERT INTO order_items (order_id, product_id, quantity) VALUES
    (1, 1, 2),
    (1, 2, 1),
    (2, 3, 5),
    (3, 1, 1);
