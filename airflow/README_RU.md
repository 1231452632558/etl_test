# Добавление Pipeline Metabase в Apache Airflow

Каталог содержит DAG `pipeline_metabase_nightly`, который ежедневно в 02:00 по
московскому времени выполняет ETL как цепочку нативных Airflow tasks. Файл
`scripts/etl_pipeline.py` DAG не вызывает и не изменяет.

Airflow управляет расписанием, журналом и повторными попытками отдельных стадий.
Полный безопасный цикл сохранен: получение дампа, staging-БД, FDW-синхронизация в
стабильную main-БД, weekly-обновление и очистка staging после успеха. При ошибке
следующие стадии не запускаются, а staging-БД и диагностические артефакты
сохраняются.

## Состав каталога

```text
airflow/
├── Dockerfile
├── dags/
│   └── pipeline_metabase_dag.py
├── runtime/
│   └── remote_postgres.py
├── .env.example
├── requirements.txt
└── README_RU.md
```

## Требования

- Apache Airflow **3.3.1** с Task SDK (`airflow.sdk`);
- Python 3.10–3.14 и `pendulum` в окружении scheduler/worker;
- `ssh` и `scp` внутри контейнера Airflow worker;
- сетевой SSH-доступ worker к целевому серверу PostgreSQL и источнику дампа;
- технический пользователь на целевом сервере, которому разрешено без пароля
  выполнять только `psql` от имени системного пользователя `postgres`;
- рабочий конфиг `etl_config.ini`, приватный SSH-ключ и заполненный
  `known_hosts` в виде Docker secrets или защищенных read-only mounts.

Если используется CeleryExecutor или KubernetesExecutor, проект, конфиг и ключи
должны быть доступны именно тому worker/pod, который выполняет задачу. Одной
установки файлов только в scheduler недостаточно.

## Как DAG получает доступ к PostgreSQL

Airflow и PostgreSQL находятся на разных серверах. Поэтому worker **не**
запускает локальный `sudo`, не монтирует PostgreSQL socket и не подключается к
порту БД напрямую. Для каждой SQL-операции используется цепочка:

```text
Airflow task в контейнере
  -> SSH под техническим пользователем
  -> целевой сервер PostgreSQL
  -> sudo -n -u postgres /usr/bin/psql
  -> локальный Unix socket PostgreSQL
```

SQL передается в stdin удаленного `psql`, включая большие файлы восстановления.
Пароль PostgreSQL в Airflow не хранится. `postgres_fdw` создается и выполняется
на целевом сервере между staging и main в одном PostgreSQL-кластере, поэтому
переливка main не идет через CSV и не проходит через Airflow worker.

Основной `scripts/pipeline_common.py` не изменен. Удаленный способ выполнения
реализован только для DAG в `airflow/runtime/remote_postgres.py`.

## 1. Подготовить runtime-конфигурацию

Не помещайте реальные пароли, приватные ключи и production-конфиг в Git.
Создайте защищенный каталог на сервере Airflow, например:

```bash
sudo install -d -m 750 -o airflow -g airflow /opt/airflow/runtime
sudo install -m 640 -o airflow -g airflow \
  /путь/к/заполненному/etl_config.ini \
  /opt/airflow/runtime/etl_config.ini
```

Пути `backup_storage_dir`, `old_backup_dir`, `temp_dir`, `log_file` и seed CSV
относятся к файловой системе worker. Для Celery/Kubernetes задачи могут попасть
на разные worker, поэтому `temp_dir`, каталоги dump и логов должны находиться на
общем persistent volume. Иначе следующая task не увидит файл предыдущей.

В секции `[database]` оставьте параметры целевого PostgreSQL, доступные уже на
целевом сервере:

```ini
[database]
db_name = main
db_user = <владелец_main_и_staging>
db_host = /var/run/postgresql
db_port = 5432
temp_db_prefix = temp_restore_
```

`db_host` здесь — путь к Unix socket **на целевом сервере**, а не внутри
контейнера Airflow. SSH-параметры задаются отдельно через Airflow Connection.

## 2. Разместить проект и DAG

Проект должен быть доступен scheduler и worker по одному и тому же внутреннему
пути. Пример для установки непосредственно на сервер:

```bash
sudo git clone <URL_ЭТОГО_РЕПОЗИТОРИЯ> /opt/airflow/pipeline-metabase
sudo chown -R airflow:airflow /opt/airflow/pipeline-metabase
ln -s \
  /opt/airflow/pipeline-metabase/airflow/dags/pipeline_metabase_dag.py \
  /opt/airflow/dags/pipeline_metabase_dag.py
```

Если репозиторий уже доставляется CI/CD, вместо `git clone` обновляйте его
штатным механизмом развертывания. DAG можно также скопировать в каталог `dags`,
но проект с `scripts/` все равно должен быть доступен worker.

## 3. Задать переменные окружения Airflow

Значения по умолчанию уже подходят путям из примера:

```bash
PIPELINE_METABASE_PROJECT_DIR=/opt/airflow/pipeline-metabase
PIPELINE_METABASE_CONFIG_FILE=/opt/airflow/runtime/etl_config.ini
PIPELINE_METABASE_DB_HOST_CONN_ID=pipeline_metabase_db_host
```

Добавьте их в окружение scheduler и всех worker. Пример находится в
`airflow/.env.example`. После изменения окружения перезапустите соответствующие
компоненты Airflow.

## 4. Настроить целевой сервер PostgreSQL

Используйте существующего технического Linux-пользователя, под которым раньше
запускался скрипт. Ниже он обозначен как `<etl_ssh_user>`.

На целевом сервере проверьте команды:

```bash
sudo -u <etl_ssh_user> sudo -n -u postgres \
  /usr/bin/psql -X -h /var/run/postgresql -p 5432 -d postgres \
  -c 'SELECT current_user;'
```

Результат должен содержать `postgres`, пароль запрашиваться не должен. Если
правила еще нет, создайте его через `visudo` в
`/etc/sudoers.d/pipeline-metabase-airflow`:

```sudoers
<etl_ssh_user> ALL=(postgres) NOPASSWD: /usr/bin/psql
```

Не выдавайте `NOPASSWD: ALL`. Убедитесь, что PostgreSQL принимает локальные
подключения пользователя `postgres` через указанный socket. Порт PostgreSQL
открывать для сервера Airflow не требуется.

Доступ к `psql` от имени `postgres` является привилегированным, даже если в
`sudoers` разрешена только одна команда. Используйте отдельный SSH-ключ только
для этого DAG, ограничьте вход на SSH firewall-ом адресом сервера Airflow и
добавьте для ключа в `authorized_keys` как минимум параметры
`from="<IP_AIRFLOW>",restrict`. Не используйте личный ключ администратора.

Для FDW администратор PostgreSQL должен один раз установить расширение в main:

```bash
sudo -u postgres /usr/bin/psql -X \
  -h /var/run/postgresql -p 5432 -d main \
  -c 'CREATE EXTENSION IF NOT EXISTS postgres_fdw;'
```

DAG создает только временный foreign server/schema и удаляет их после загрузки.

## 5. Настроить SSH Connection в Airflow

Создайте Airflow Connection с ID `pipeline_metabase_db_host`. Достаточно типа
`Generic`: DAG использует публичный `Connection` API Airflow 3.3.1 и системный
OpenSSH client, а не `PostgresHook`.

| Поле | Значение |
|---|---|
| Host | DNS-имя или IP целевого сервера PostgreSQL |
| Login | `<etl_ssh_user>` |
| Port | SSH-порт, обычно `22` |
| Password | не заполнять; используется SSH-ключ |
| Schema | не используется |

Поле Extra:

```json
{
  "identity_file": "/run/secrets/pipeline_db_ssh_key",
  "known_hosts_file": "/opt/airflow/ssh/known_hosts",
  "db_socket": "/var/run/postgresql",
  "db_port": 5432,
  "connect_timeout": 15,
  "restore_timeout_seconds": 14400,
  "remote_sudo": "/usr/bin/sudo",
  "remote_psql": "/usr/bin/psql",
  "remote_db_os_user": "postgres"
}
```

Пути относятся к контейнеру worker. Connection можно хранить в metadata DB,
переменной `AIRFLOW_CONN_PIPELINE_METABASE_DB_HOST` или внешнем Secrets Backend.
Не помещайте приватный ключ в Connection, репозиторий или JSON Extra.
`restore_timeout_seconds` ограничивает потоковое восстановление одного SQL-файла;
по умолчанию это 4 часа. SSH keepalive включен в adapter, поэтому длительное
восстановление не зависит от короткого простоя соединения.

## 6. Настроить Docker Compose

Файл `airflow/Dockerfile` добавляет в Airflow 3.3.1 только OpenSSH client. `sudo`,
`psql` и пользователь `postgres` внутри контейнера больше не нужны.

Пример для worker:

Перед запуском разместите отдельный ключ так, чтобы его мог читать UID
пользователя `airflow` в контейнере, но не другие пользователи хоста. Для
официального образа UID по умолчанию равен `50000`:

```bash
sudo install -o 50000 -g 0 -m 0400 \
  /защищенный/источник/pipeline_db_ssh_key \
  /srv/airflow-secrets/pipeline_db_ssh_key
sudo install -o 50000 -g 0 -m 0444 \
  /защищенный/источник/known_hosts \
  /srv/airflow-ssh/known_hosts
```

Если в Compose задан другой `AIRFLOW_UID`, используйте его вместо `50000`.

```yaml
services:
  airflow-worker:
    build:
      context: /srv/pipeline-metabase
      dockerfile: airflow/Dockerfile
    environment:
      PIPELINE_METABASE_PROJECT_DIR: /opt/airflow/pipeline-metabase
      PIPELINE_METABASE_CONFIG_FILE: /opt/airflow/runtime/etl_config.ini
      PIPELINE_METABASE_DB_HOST_CONN_ID: pipeline_metabase_db_host
    volumes:
      - /srv/pipeline-metabase:/opt/airflow/pipeline-metabase:ro
      - /srv/airflow-runtime:/opt/airflow/runtime:ro
      - pipeline-etl-data:/opt/airflow/pipeline-data
      - /srv/airflow-ssh/known_hosts:/opt/airflow/ssh/known_hosts:ro
      - /srv/airflow-secrets/pipeline_db_ssh_key:/run/secrets/pipeline_db_ssh_key:ro

volumes:
  pipeline-etl-data:
```

Пути `[paths]` в `etl_config.ini` направьте в
`/opt/airflow/pipeline-data/...`. Тот же volume должен быть подключен ко всем
worker, которые могут исполнять задачи DAG. Обычный локальный named volume не
является общим для worker на разных Docker-хостах — там нужен NFS/CephFS или
другое общее хранилище.

Пересоберите исполняющий компонент:

```bash
docker compose build --no-cache airflow-worker
docker compose up -d airflow-worker
```

При LocalExecutor образ, mounts и environment задаются scheduler, потому что
именно он исполняет задачи.

## 7. Проверить SSH и PostgreSQL до запуска DAG

Сначала проверьте файлы и SSH из исполняющего контейнера:

```bash
docker compose exec airflow-worker \
  test -r /run/secrets/pipeline_db_ssh_key
docker compose exec airflow-worker \
  test -r /opt/airflow/ssh/known_hosts
docker compose exec airflow-worker \
  ssh -o BatchMode=yes -o StrictHostKeyChecking=yes \
  -o UserKnownHostsFile=/opt/airflow/ssh/known_hosts \
  -i /run/secrets/pipeline_db_ssh_key \
  <etl_ssh_user>@<db_host> \
  'sudo -n -u postgres /usr/bin/psql -X -h /var/run/postgresql -p 5432 -d postgres -c "SELECT current_user;"'
```

Команда должна вернуть `postgres`. Типовые ошибки:

| Сообщение | Причина |
|---|---|
| `Permission denied (publickey)` | неверный ключ, Login или `authorized_keys` на целевом сервере |
| `Host key verification failed` | отсутствует/не совпадает запись в `known_hosts` |
| `sudo: a password is required` | правило `NOPASSWD` не действует для SSH-пользователя |
| `psql: command not found` | в Connection Extra указан неверный `remote_psql` |
| `Peer authentication failed` | не подходит локальное правило `pg_hba.conf` на сервере PostgreSQL |
| `could not connect ... socket` | неверны `db_socket`/`db_port` или PostgreSQL не запущен |

## 8. Проверить DAG до включения расписания

Выполните команды от имени пользователя и в окружении Airflow:

```bash
airflow dags list-import-errors --local
airflow dags list --local | grep pipeline_metabase_nightly
airflow tasks test pipeline_metabase_nightly prepare_run_context 2026-09-07
```

Команда `tasks test` запускает реальную первую стадию ETL. Сначала используйте
тестовый конфиг и тестовую PostgreSQL. Затем в Airflow UI проверьте граф DAG и
прогоните полный тестовый DagRun, прежде чем включать production-расписание.

Для ручного production-запуска используйте интерфейс Airflow либо `airflowctl`
(в Airflow 3 удалённые операции вынесены из локального `airflow` CLI):

```bash
airflowctl dagrun trigger --dag-id pipeline_metabase_nightly
```

## Эксплуатационные настройки

- Расписание задается в DAG строкой `0 2 * * *`, часовой пояс —
  `Europe/Moscow`.
- `catchup=False`: пропущенные исторические дни автоматически не догоняются.
- `max_active_runs=1`: два полных запуска DAG не выполняются одновременно.
- Одна повторная попытка каждой стадии выполняется через 15 минут. Staging-имя
  стабильно при retry одного DagRun, а FDW/weekly операции идемпотентны.
- Для крупных FDW-операций продолжает действовать лимит snapshot-транзакции из
  `etl_config.ini` (по умолчанию 4 часа).
- Лог Airflow показывает stdout/stderr, а подробный ETL-лог сохраняется по пути
  `[paths].log_file` из `etl_config.ini`.

Чтобы изменить расписание, отредактируйте параметр `schedule` в
`airflow/dags/pipeline_metabase_dag.py`, проверьте импорт и доставьте новую версию
DAG штатным способом.

## Граф и контекст выполнения

```text
prepare_run_context -> download_and_extract -> restore_staging
  -> build_fdw_snapshot_plan -> load_snapshot_to_main -> update_weekly_tables
  -> publish_verified_backup -> cleanup_successful_run
```

Через XCom передаются только пути, имя staging-БД и FDW-план. Секреты и объекты
подключения не передаются: каждая задача отдельно читает защищенный
`etl_config.ini` и Airflow Connection в своем worker. При сбое
`cleanup_successful_run` не выполняется, поэтому staging и распакованный dump
остаются для диагностики.

## Отдельный DAG для Asterisk

Задача `asterisk_cdr` полностью исключена из этого DAG. Получение CSV и его
идемпотентная загрузка по `id` в `asterisk_cdr` стабильной main-БД должны быть
реализованы в отдельном Asterisk DAG. Этот DAG не ждёт его завершения и не
создаёт междаговую зависимость.
