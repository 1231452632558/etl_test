-- Тестовый дамп версии 1 (начальный)
-- Для инициализации основной базы данных

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER DEFAULT 1,
    price DECIMAL(10, 2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2),
    stock INTEGER DEFAULT 0
);

-- Начальные данные: пользователи
INSERT INTO users (id, name, email) VALUES
(1, 'Иван Петров', 'ivan@example.com'),
(2, 'Мария Сидорова', 'maria@example.com'),
(3, 'Алексей Смирнов', 'alexey@example.com');

-- Начальные данные: продукты
INSERT INTO products (id, name, description, price, stock) VALUES
(1, 'Ноутбук', 'Мощный ноутбук для работы', 75000.00, 10),
(2, 'Мышь беспроводная', 'Эргономичная мышь', 2500.00, 50),
(3, 'Клавиатура механическая', 'RGB подсветка', 8000.00, 25);

-- Начальные данные: заказы
INSERT INTO orders (id, user_id, product_name, quantity, price) VALUES
(1, 1, 'Ноутбук', 1, 75000.00),
(2, 2, 'Мышь беспроводная', 2, 5000.00);
