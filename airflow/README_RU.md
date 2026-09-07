# Добавление Pipeline Metabase в Apache Airflow

Каталог содержит DAG `pipeline_metabase_nightly`, который ежедневно в 02:00 по
московскому времени запускает существующий `scripts/etl_pipeline.py`.

Airflow управляет расписанием, журналом и одной повторной попыткой. Сам ETL
по-прежнему выполняет полный безопасный цикл: получение дампа, staging-БД,
FDW-синхронизация в стабильную main-БД, weekly-обновление и очистка staging после
успеха. При ошибке процесс возвращает ненулевой код, задача Airflow становится
`failed`, а диагностические артефакты и staging-БД сохраняются.

## Состав каталога

```text
airflow/
├── dags/
│   └── pipeline_metabase_dag.py
├── .env.example
├── requirements.txt
└── README_RU.md
```

## Требования

- Apache Airflow 2.x с доступным `BashOperator`;
- Python 3 и `pendulum` в окружении scheduler/worker;
- `psql`, `scp`, `ssh` и остальные системные команды, используемые ETL;
- `sudo` и разрешение без пароля выполнять `sudo -u postgres psql` для
  пользователя Airflow (это требование текущего `pipeline_common.py`);
- сетевой доступ от Airflow worker к PostgreSQL и серверу с дампом;
- рабочий конфиг `etl_config.ini`, SSH-ключ и способ аутентификации PostgreSQL.

Если используется CeleryExecutor или KubernetesExecutor, проект, конфиг и ключи
должны быть доступны именно тому worker/pod, который выполняет задачу. Одной
установки файлов только в scheduler недостаточно.

## 1. Подготовить runtime-конфигурацию

Не помещайте реальные пароли, приватные ключи и production-конфиг в Git.
Создайте защищенный каталог на сервере Airflow, например:

```bash
sudo install -d -m 750 -o airflow -g airflow /opt/airflow/runtime
sudo install -m 640 -o airflow -g airflow \
  /путь/к/заполненному/etl_config.ini \
  /opt/airflow/runtime/etl_config.ini
```

Проверьте в конфиге пути `backup_storage_dir`, `old_backup_dir`, `temp_dir`,
`log_file` и CSV-файлы: они должны существовать и быть доступны пользователю,
под которым выполняется Airflow task.

Для PostgreSQL используйте один из вариантов:

- Unix socket и разрешенную сервером аутентификацию;
- файл `.pgpass` с правами `0600` у пользователя Airflow;
- переменную `PGPASSFILE`, указывающую на смонтированный secret-файл.

Для `scp` установите приватный ключ как secret и заранее добавьте ключ удаленного
хоста в `known_hosts`. Не отключайте проверку host key.

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
PIPELINE_METABASE_PYTHON=python3
```

Добавьте их в окружение scheduler и всех worker. Пример находится в
`airflow/.env.example`. После изменения окружения перезапустите соответствующие
компоненты Airflow.

## 4. Вариант для Docker Compose

Добавьте одинаковые mounts и environment как минимум в `airflow-scheduler` и
`airflow-worker` (а при LocalExecutor — в компонент, исполняющий задачи):

```yaml
services:
  airflow-scheduler:
    volumes:
      - /srv/pipeline-metabase:/opt/airflow/pipeline-metabase:ro
      - /srv/airflow-runtime:/opt/airflow/runtime:ro
    environment:
      PIPELINE_METABASE_PROJECT_DIR: /opt/airflow/pipeline-metabase
      PIPELINE_METABASE_CONFIG_FILE: /opt/airflow/runtime/etl_config.ini
      PIPELINE_METABASE_PYTHON: python3

  airflow-worker:
    volumes:
      - /srv/pipeline-metabase:/opt/airflow/pipeline-metabase:ro
      - /srv/airflow-runtime:/opt/airflow/runtime:ro
    environment:
      PIPELINE_METABASE_PROJECT_DIR: /opt/airflow/pipeline-metabase
      PIPELINE_METABASE_CONFIG_FILE: /opt/airflow/runtime/etl_config.ini
      PIPELINE_METABASE_PYTHON: python3
```

Запись логов и временных файлов требует writable-каталогов. Поэтому пути из
`etl_config.ini` нужно отдельно смонтировать с правом записи. Если worker должен
подключаться к PostgreSQL через Unix socket, смонтируйте также каталог socket;
иначе настройте TCP-подключение и безопасную аутентификацию.

Стандартный Docker-образ Airflow может не содержать `sudo` и системного
пользователя `postgres`. В таком случае приведенных mounts недостаточно: нужен
корпоративный образ worker с PostgreSQL client, `sudo`, пользователем `postgres`
и минимальным правилом `sudoers`, разрешающим только требуемый вызов `psql`.
Не выдавайте пользователю Airflow неограниченный `NOPASSWD: ALL`.

## 5. Проверить DAG до включения расписания

Выполните команды от имени пользователя и в окружении Airflow:

```bash
airflow dags list-import-errors
airflow dags list | grep pipeline_metabase_nightly
airflow tasks test pipeline_metabase_nightly run_incremental_pipeline 2026-09-07
```

Команда `tasks test` запускает реальный ETL. Сначала используйте тестовый конфиг
и тестовую PostgreSQL. После успешного теста откройте Airflow UI, найдите
`pipeline_metabase_nightly` и включите DAG.

Для ручного production-запуска после проверки:

```bash
airflow dags trigger pipeline_metabase_nightly
```

## Эксплуатационные настройки

- Расписание задается в DAG строкой `0 2 * * *`, часовой пояс —
  `Europe/Moscow`.
- `catchup=False`: пропущенные исторические дни автоматически не догоняются.
- `max_active_runs=1`: два полных запуска DAG не выполняются одновременно.
- Одна повторная попытка выполняется через 15 минут.
- Предельное время задачи — 6 часов. Оно больше текущего лимита одной
  snapshot-транзакции в ETL (4 часа).
- Лог Airflow показывает stdout/stderr, а подробный ETL-лог сохраняется по пути
  `[paths].log_file` из `etl_config.ini`.

Чтобы изменить расписание, отредактируйте параметр `schedule` в
`airflow/dags/pipeline_metabase_dag.py`, проверьте импорт и доставьте новую версию
DAG штатным способом.

## Почему ETL представлен одной задачей Airflow

Стадии ETL передают друг другу runtime-контекст: путь принятого архива, имя
созданной staging-БД и рассчитанный FDW-план. Текущая реализация также выполняет
табличные транзакции и сохраняет диагностическое состояние при сбое. Один
`BashOperator` не разрывает этот контракт и позволяет повторно использовать уже
проверенный CLI без дублирования бизнес-логики в DAG.
