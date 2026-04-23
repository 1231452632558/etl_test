-- Тестовый дамп версии 2 (ночной дамп с изменениями)
-- Содержит: новые строки, измененные строки, удаленные строки (не синхронизируются)

-- Таблицы создаются если не существуют (для чистого восстановления)
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

-- Обновленные данные: пользователи
-- Изменен пользователь 2 (Мария Сидорова -> Мария Иванова)
-- Добавлен новый пользователь 4
INSERT INTO users (id, name, email) VALUES
(1, 'Иван Петров', 'ivan@example.com'),
(2, 'Мария Иванова', 'maria.new@example.com'),  -- ИЗМЕНЕНИЕ: фамилия и email
(3, 'Алексей Смирнов', 'alexey@example.com'),
(4, 'Дмитрий Козлов', 'dmitry@example.com');    -- НОВЫЙ пользователь

-- Обновленные данные: продукты
-- Изменена цена на продукт 1
-- Добавлен новый продукт 4
-- Уменьшен stock у продукта 2
INSERT INTO products (id, name, description, price, stock) VALUES
(1, 'Ноутбук Pro', 'Мощный ноутбук для профессионалов', 85000.00, 8),  -- ИЗМЕНЕНИЕ: название, описание, цена, stock
(2, 'Мышь беспроводная', 'Эргономичная мышь', 2500.00, 45),             -- ИЗМЕНЕНИЕ: stock уменьшен
(3, 'Клавиатура механическая', 'RGB подсветка', 8000.00, 25),
(4, 'Монитор 27"', '4K UHD монитор', 35000.00, 15);                     -- НОВЫЙ продукт

-- Обновленные данные: заказы
-- Добавлены новые заказы
-- Изменен заказ 2 (количество)
INSERT INTO orders (id, user_id, product_name, quantity, price) VALUES
(1, 1, 'Ноутбук', 1, 75000.00),
(2, 2, 'Мышь беспроводная', 5, 12500.00),   -- ИЗМЕНЕНИЕ: количество и цена
(3, 3, 'Клавиатура механическая', 1, 8000.00),  -- НОВЫЙ заказ
(4, 4, 'Монитор 27"', 2, 70000.00);            -- НОВЫЙ заказ от нового пользователя
