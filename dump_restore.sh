#!/bin/bash

# Параметры
masked
# Функция для записи в лог
log_message() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    if echo "$message" | grep -i -q "already exists\|already exists"; then return 0
    fi
    echo "$timestamp - $message" | tee -a "$LOG_FILE"
}
#функция выгрузки и загрузки данных в csv и добавления новых строк в периодические
download_data(){
	 log_message "=== Шаг 1: Выгрузка данных в CSV (перед сбросом) ==="
    # Проверка: есть ли данные вообще?
    local count1=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM group_employee_count;" | tr -d ' ')
    local count2=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM users_active;" | tr -d ' ')
    log_message "В БД найдено: group_employee_count=$count1, users_active=$count2"

    # Выгрузка через STDOUT (надежно для прав доступа)
    sudo -u postgres psql -d "$DB_NAME" -c "\copy (SELECT * FROM group_employee_count) TO STDOUT WITH (FORMAT CSV, HEADER true)" > "$CSV_FILE1" 2>> "$LOG_FILE"

    sudo -u postgres psql -d "$DB_NAME" -c "\copy (SELECT * FROM users_active) TO STDOUT WITH (FORMAT CSV, HEADER true)"  > "$CSV_FILE2" 2>> "$LOG_FILE"

    # Проверка результата
    if [ -s "$CSV_FILE1" ] && [ -s "$CSV_FILE2" ]; then
        log_message "Выгрузка успешна. Файлы созданы."
    else
        log_message "ОШИБКА: Файлы пустые или не созданы!"
        # Не делаем exit 1, если база может быть пуста изначально
    fi
}
upload_data(){
	#local count1 $(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM group_employee_count;" | tr -d ' ')
        #local count2=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM users_active;" | tr -d ' ')

        if [ -f "$CSV_FILE1" ] && [ -f "$CSV_FILE2" ]; then
	    log_message "Шаг 5: Загрузка данных из $CSV_FILE1 в таблицу group_employee_count..."
	    # Загружаем данные из CSV в таблицу с помощью COPY команды в psql
	    sudo -u postgres psql -d "$DB_NAME" -c "\copy group_employee_count FROM '$CSV_FILE1' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');" 2>>"$LOG_FILE" || true
	    sudo -u postgres psql -d "$DB_NAME" -c "\copy users_active FROM '$CSV_FILE2' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');" 2>>"$LOG_FILE" || true
	    log_message "Шаг 5: Данные загружены из $CSV_FILE1|$CSV_FILE2"
	else
	    log_message "Шаг 5: Один из файлов не найден ($CSV_FILE1 | $CSV_FILE2), загрузка пропущена"
	    log_message "  - $CSV_FILE1 : $([ -f "$CSV_FILE1" ] && echo 'OK' || echo 'НЕТ')"
	    log_message "  - $CSV_FILE2 : $([ -f "$CSV_FILE2" ] && echo 'OK' || echo 'НЕТ')"
        #       log_message "Шаг 5: Файл $CSV_FILE1,$CSV_FILE2 не найден, пропускаем загрузку данных"
	fi
	if [ "$CURRENT_DAY" -eq "$TARGET_DAY" ]; then
		log_message "Шаг 5: Еженедельный. Добавление строк с актуальным количеством в группах masked и активных пользователей"
		sudo -u postgres psql -d "$DB_NAME" -c
                "WITH max_id AS (
    		SELECT COALESCE(MAX(id), 0) AS val FROM group_employee_count
		),
		new_rows AS (
		    SELECT
                	m.val + ROW_NUMBER() OVER() AS new_id,
			g.id as gid, g.lastname as gname, COUNT(DISTINCT u.id) as u_count
	        	FROM users u, max_id m
	        	JOIN groups_users gu ON u.id = gu.user_id
	        	JOIN users g ON gu.group_id = g.id
	        	WHERE u.status = 1 and g.lastname ILIKE 'masked'
	        	GROUP BY gid, gname
		)
		INSERT INTO group_employee_count (id, group_id, group_name, snapshot_date, user_count)
		SELECT new_id, gid, gname, CURRENT_DATE, u_count" 2>>"$LOG_FILE" || true

	    sudo -u postgres psql -d "$DB_NAME" -c "INSERT INTO users_active (snapshot_date, user_count) SELECT  CURRENT_DATE, COUNT(DISTINCT u.id) FROM users u WHERE u.status=1;"
	else
	    log_message "Шаг 5: Новые строки не добавлялись"
	fi
}
# Функция для завершения всех соединений с базой
terminate_connections() {
    log_message "Завершение всех соединений с базой $DB_NAME..."
    # Завершаем все соединения с базой (кроме текущего соединения с postgres)
    sudo -u postgres psql -c "
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '$DB_NAME'
        AND pid <> pg_backend_pid();" 2>>"$LOG_FILE" || true
    # Ждем немного, чтобы соединения завершились
    sleep 2
}

# Функция для сохранения старых бэкапов
rotate_backups() {
    local backup_dir="$BACKUP_STORAGE_DIR"
    local tar_file="$BACKUP_TAR"
    log_message "Шаг 0: Ротация старых бэкапов..."
    # Создаем директорию для хранения бэкапов
    mkdir -p "$backup_dir"
    # Проверяем, есть ли уже существующие файлы
    if [ -f "$backup_dir/$sql_file" ] && [ -f "$backup_dir/$tar_file" ]; then
        log_message "Найдены существующие файлы бэкапа, перемещаем в архив..."
        # Создаем timestamp для старых бэкапов
        local timestamp=$(date +%Y%m%d_%H%M%S)
        # Создаем директорию для старой копии
        local old_backup_dir="$backup_dir/old_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$old_backup_dir"
        # Перемещаем старые файлы
        mv "$backup_dir/$sql_file" "$old_backup_dir/"
        mv "$backup_dir/$tar_file" "$old_backup_dir/"
        log_message "Старые файлы перемещены в: $old_backup_dir"
    fi
    # Удаляем старые резервные копии, оставляем только MAX_BACKUPS
    local old_dirs=($(find "$backup_dir" -maxdepth 1 -name "old_*" -type d -printf '%T@ %p\n' | sort -nr | cut -d' ' -f2-))
    local total_old=${#old_dirs[@]}
    if [ $total_old -gt $((MAX_BACKUPS - 1)) ]; then
        local to_delete=$((total_old - MAX_BACKUPS + 1))
        log_message "Удаление $to_delete старых копий бэкапов..."
        for ((i = $((MAX_BACKUPS - 1)); i < $total_old; i++)); do
            local dir_to_delete="${old_dirs[$i]}"
            if [ -d "$dir_to_delete" ]; then
                log_message "Удаляю старую копию: $dir_to_delete"
                rm -rf "$dir_to_delete"
            fi
        done
        log_message "Ротация бэкапов завершена"
    else
        log_message "Ротация не требуется, всего старых копий: $((total_old))"
    fi
}

# Начало выполнения скрипта
log_message "=== НАЧАЛО ВОССТАНОВЛЕНИЯ БЭКАПА ==="
log_message "Дата запуска: $(date)"
log_message "Пользователь запуска: $(whoami)"
log_message "Хост: $(hostname)"
log_message "Директория хранения бэкапов: $BACKUP_STORAGE_DIR"
log_message "Максимальное количество копий: $MAX_BACKUPS"

# 0.Выполняем ротацию бэкапов
rotate_backups

# 1. Копирование файлов с удаленного сервера
log_message "Шаг 1: Копирование файлов с удаленного сервера..."
log_message "Шаг 1: Источник: $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH"
log_message "Шаг 1: Назначение: $BACKUP_STORAGE_DIR"

if scp "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH$BACKUP_TAR" "$BACKUP_STORAGE_DIR/"; then
    log_message "Шаг 1:Файл $BACKUP_TAR успешно скопирован"
else
    log_message "Шаг1: Ошибка копирования файла $BACKUP_TAR"
    exit 1
fi

# Проверка наличия архива
if [ ! -f "$BACKUP_STORAGE_DIR/$BACKUP_TAR" ]; then
    log_message "Файл отсутствует в директории хранения"
    exit 1
fi

log_message "Шаг 1: Успешно скопирован архив в $BACKUP_STORAGE_DIR"
# 2. Удаление существующей базы (если существует)
log_message "Шаг 2: Выгрузка данных, Завершение соединений и удаление существующей базы $DB_NAME..."
download_data
terminate_connections "$DB_NAME"
log_message "Шаг 2: Удаление существующей базы $DB_NAME..."
sudo -u postgres psql -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" 2>>"$LOG_FILE" || true
log_message "Шаг 2: Существующая база удалена (если существовала)"

# 3. Создание новой базы с владельцем
log_message "Шаг 3: Создание новой базы $DB_NAME с владельцем $DB_USER..."
sudo -u postgres psql -c "CREATE DATABASE \"$DB_NAME\" OWNER \"$DB_USER\";" 2>>"$LOG_FILE"
#sudo -u postgres psql -d "$DB_NAME" -c "GRANT ALL PRIVILEGES ON DATABASE \"$DB_NAME\" TO \"$DB_USER\";" 2>>"$LOG_FILE" || true
if [ $? -eq 0 ]; then
    log_message "Шаг 3: Новая база создана с владельцем $DB_USER"
else
    log_message "Шаг 3: Ошибка при создании новой базы"
    exit 1
fi
# 4. Восстановление дампа из tar.gz
log_message "Шаг 4: Восстановление дампа из архива..."

# Распаковка архива во временную директорию
TAR_EXTRACT_DIR="/tmp/pg_lob_extract_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$TAR_EXTRACT_DIR"

if tar -xzf "$BACKUP_STORAGE_DIR/$BACKUP_TAR" -C "$TAR_EXTRACT_DIR"; then
    log_message "Архив успешно распакован в $TAR_EXTRACT_DIR"
    # Найдем и выполним все SQL файлы из архива
    for sql_file in "$TAR_EXTRACT_DIR"/*.sql; do
        if [ -f "$sql_file" ]; then
            log_message "Шаг 4: Выполняю файл: $(basename "$sql_file")"
            if sudo -u postgres psql -d "$DB_NAME" -f "$sql_file" 2> >(grep -v -i "already exists" >> "$LOG_FILE") || true; then
                log_message "Шаг 4: Файл $(basename "$sql_file") успешно выполнен"
            else
                log_message "Шаг 4: Ошибка выполнения файла $(basename "$sql_file")"
                # Не выходим с ошибкой, продолжаем с другими файлами
            fi
        fi
    done
    # Удалим временную директорию
    rm -rf "$TAR_EXTRACT_DIR"
else
    log_message "Ошибка распаковки архива"
    exit 1
fi
log_message "Шаг 5: Создание таблицы asterisk_cdr и group_employee_count и загрузка данных из CSV..."
# Создаем таблицу asterisk_cdr
sudo -u postgres psql -d "$DB_NAME" -c "
CREATE TABLE IF NOT EXISTS asterisk_cdr (
    id INTEGER PRIMARY KEY,
    calldate TIMESTAMP WITHOUT TIME ZONE,
    clid TEXT,
    src TEXT,
    dst TEXT,
    dcontext TEXT,
    channel TEXT,
    dstchannel TEXT,
    lastapp TEXT,
    lastdata TEXT,
    duration INTEGER,
    billsec INTEGER,
    disposition TEXT,
    amaflags TEXT,
    accountcode TEXT,
    uniqueid TEXT,
    userfield TEXT,
    cdr_start TIMESTAMP WITHOUT TIME ZONE,
    answer TIMESTAMP WITHOUT TIME ZONE,
    cdr_end TIMESTAMP WITHOUT TIME ZONE,
    linkedid TEXT,
    peeraccount TEXT,
    sequence TEXT
);" 2>>"$LOG_FILE" || true

sudo -u postgres psql -d "$DB_NAME" -c "CREATE TABLE IF NOT EXISTS group_employee_count (
    id SERIAL PRIMARY KEY,
    group_id INTEGER NOT NULL,
    group_name VARCHAR(255) NOT NULL,
    snapshot_date DATE NOT NULL,
    user_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (group_id, snapshot_date)
);" 2>>"$LOG_FILE" || true

sudo -u postgres psql -d "$DB_NAME" -c "CREATE TABLE IF NOT EXISTS users_active (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    user_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (id, snapshot_date)
);" 2>>"$LOG_FILE" || true

log_message "Шаг 5: Таблица asterisk_cdr и group_employee_count|users_active создана (если не существовала)"
if [ -f "$CSV_FILE" ]; then
    log_message "Шаг 5: Загрузка данных из $CSV_FILE в таблицу asterisk_cdr..."
    # Загружаем данные из CSV в таблицу с помощью COPY команды в psql
    sudo -u postgres psql -d "$DB_NAME" -c "\copy asterisk_cdr FROM '$CSV_FILE' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');" 2>>"$LOG_FILE" || true
    log_message "Шаг 5: Данные загружены из $CSV_FILE"
else
    log_message "Шаг 5: Файл $CSV_FILE не найден, пропускаем загрузку данных"
fi

log_message "Шаг 5: Добавление столбца project_id и обновление данных..."
sudo -u postgres psql -d "$DB_NAME" -c "ALTER TABLE asterisk_cdr ADD COLUMN project_id INTEGER;" 2>>"$LOG_FILE" || true
log_message "Шаг 5: Столбец project_id добавлен в таблицу asterisk_cdr"
sudo -u postgres psql -d "$DB_NAME" -c "UPDATE asterisk_cdr SET project_id = 3840;" 2>>"$LOG_FILE" || true
upload_data
log_message "Шаг 6: Восстановление прав для пользователя"
sudo -u postgres psql -d "$DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$DB_USER\";" 2>>"$LOG_FILE" || true

# 7. Информация о сохраненных бэкапах
log_message "Шаг 7: Информация о сохраненных бэкапах..."
log_message "Текущие файлы бэкапа:"
log_message "- $BACKUP_STORAGE_DIR/$BACKUP_SQL (текущая копия)"
log_message "- $BACKUP_STORAGE_DIR/$BACKUP_TAR (текущая копия)"

# Показываем список старых копий
local old_copies=($(find "$BACKUP_STORAGE_DIR" -maxdepth 1 -name "old_*" -type d | sort -r))
if [ ${#old_copies[@]} -gt 0 ]; then
    log_message "Сохраненные старые копии ($MAX_BACKUPS максимально):"
    for copy in "${old_copies[@]}"; do
        local copy_date=$(basename "$copy" | cut -d'_' -f2-)
        log_message "  - $copy (от $copy_date)"
    done
else
    log_message "Старые копии отсутствуют"
fi

# 8. Завершение
log_message "=== ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО ==="
log_message "Дата завершения: $(date)"
log_message "Файл лога: $LOG_FILE"
log_message "Текущие бэкапы сохранены в: $BACKUP_STORAGE_DIR"
