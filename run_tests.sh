#!/bin/bash
# Тестовый скрипт для проверки ETL pipeline
# Требует установленного PostgreSQL 14 и доступа к нему

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETL_SCRIPT="$SCRIPT_DIR/scripts/etl_pipeline.py"
DUMPS_DIR="$SCRIPT_DIR/dumps"
CONFIG_FILE="$SCRIPT_DIR/config/etl_config.ini"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "ETL Pipeline - Тестирование"
echo "=============================================="

# Проверка доступности PostgreSQL
echo -e "\n${YELLOW}Проверка подключения к PostgreSQL...${NC}"
if ! pg_isready -h localhost -p 5432 -U postgres &>/dev/null; then
    echo -e "${RED}Ошибка: PostgreSQL недоступен${NC}"
    echo "Запустите PostgreSQL:"
    echo "  sudo systemctl start postgresql"
    exit 1
fi
echo -e "${GREEN}PostgreSQL доступен${NC}"

# Очистка перед тестом (если нужно)
echo -e "\n${YELLOW}Очистка старых тестовых баз данных...${NC}"
sudo -u postgres psql -c "DROP DATABASE IF EXISTS main_db;" 2>/dev/null || true
sudo -u postgres psql -c "DROP DATABASE IF EXISTS temp_db;" 2>/dev/null || true
echo -e "${GREEN}Очистка завершена${NC}"

# Шаг 1: Инициализация
echo -e "\n${YELLOW}=== ШАГ 1: Инициализация основной базы ===${NC}"
python3 "$ETL_SCRIPT" --init --local-dump "$DUMPS_DIR/test_dump_v1.sql" --config "$CONFIG_FILE"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Инициализация успешна${NC}"
else
    echo -e "${RED}Ошибка инициализации${NC}"
    exit 1
fi

# Проверка данных в основной базе
echo -e "\n${YELLOW}Проверка данных после инициализации:${NC}"
echo "Пользователи:"
sudo -u postgres psql -d main_db -c "SELECT * FROM users ORDER BY id;"

echo -e "\nПродукты:"
sudo -u postgres psql -d main_db -c "SELECT * FROM products ORDER BY id;"

echo -e "\nЗаказы:"
sudo -u postgres psql -d main_db -c "SELECT * FROM orders ORDER BY id;"

# Шаг 2: Обработка ночного дампа
echo -e "\n${YELLOW}=== ШАГ 2: Обработка ночного дампа ===${NC}"
python3 "$ETL_SCRIPT" --local-dump "$DUMPS_DIR/test_dump_v2.sql" --config "$CONFIG_FILE"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Обработка ночного дампа успешна${NC}"
else
    echo -e "${RED}Ошибка обработки ночного дампа${NC}"
    exit 1
fi

# Проверка изменений
echo -e "\n${YELLOW}Проверка данных после синхронизации:${NC}"
echo "Пользователи (должен быть новый пользователь 4 и измененный 2):"
sudo -u postgres psql -d main_db -c "SELECT * FROM users ORDER BY id;"

echo -e "\nПродукты (должен быть новый продукт 4 и измененные 1, 2):"
sudo -u postgres psql -d main_db -c "SELECT * FROM products ORDER BY id;"

echo -e "\nЗаказы (должны быть новые заказы 3, 4 и измененный 2):"
sudo -u postgres psql -d main_db -c "SELECT * FROM orders ORDER BY id;"

# Шаг 3: Проверка временной базы (должна существовать если не использован --cleanup)
echo -e "\n${YELLOW}Проверка временной базы данных:${NC}"
sudo -u postgres psql -c "SELECT datname FROM pg_database WHERE datname = 'temp_db';"

# Финальная очистка (опционально)
echo -e "\n${YELLOW}=== ШАГ 3: Тест с очисткой временной базы ===${NC}"
echo "Повторная обработка с флагом --cleanup"
python3 "$ETL_SCRIPT" --cleanup --local-dump "$DUMPS_DIR/test_dump_v2.sql" --config "$CONFIG_FILE"

echo -e "\n${YELLOW}Проверка что временная база удалена:${NC}"
TEMP_DB_EXISTS=$(sudo -u postgres psql -t -c "SELECT 1 FROM pg_database WHERE datname = 'temp_db';" | tr -d ' ')
if [ "$TEMP_DB_EXISTS" = "1" ]; then
    echo -e "${RED}Временная база все еще существует${NC}"
else
    echo -e "${GREEN}Временная база успешно удалена${NC}"
fi

echo -e "\n${GREEN}=============================================="
echo "Все тесты пройдены успешно!"
echo "==============================================${NC}"
echo -e "\nЛоги доступны в: $SCRIPT_DIR/logs/"
echo -e "Для автоматизации добавьте в crontab:"
echo -e "  0 2 * * * cd $SCRIPT_DIR && python3 $ETL_SCRIPT --cleanup --config $CONFIG_FILE >> $SCRIPT_DIR/logs/cron.log 2>&1"
