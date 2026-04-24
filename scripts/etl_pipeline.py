#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline - Модульная версия (оркестратор задач)
Запускает ETL pipeline как последовательность независимых задач
"""

import sys
import os
import argparse
from pathlib import Path

# Добавляем родительскую директорию в path для импорта модулей
sys.path.insert(0, str(Path(__file__).parent))

from tasks import (
    TaskRunner,
    ExtractTask,
    RestoreTask,
    CompareTask,
    TransformCustomValuesTask,
    LoadTask,
    WeeklyTask,
    CleanupTask
)


def main():
    parser = argparse.ArgumentParser(
        description='ETL Pipeline для PostgreSQL (Модульная версия)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --init /path/to/dump.tar.gz     # Первый запуск (инициализация)
  %(prog)s /path/to/dump.tar.gz            # Ночная обработка
  %(prog)s --cleanup /path/to/dump.tar.gz  # Ночная обработка с очисткой
        """
    )
    
    parser.add_argument(
        'dump_file',
        nargs='?',
        help='Путь к файлу дампа (.tar.gz или .sql)'
    )
    
    parser.add_argument(
        '--init',
        action='store_true',
        help='Режим инициализации (первый запуск)'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Очистить временную базу после завершения'
    )
    
    parser.add_argument(
        '--config',
        default='/workspace/config/etl_config.ini',
        help='Путь к конфигурационному файлу (по умолчанию: /workspace/config/etl_config.ini)'
    )
    
    args = parser.parse_args()
    
    if not args.dump_file:
        parser.print_help()
        print("\nОшибка: Необходимо указать путь к файлу дампа")
        sys.exit(1)
    
    # Проверяем существование файла дампа
    if not os.path.exists(args.dump_file):
        print(f"Ошибка: Файл дампа не найден: {args.dump_file}")
        sys.exit(1)
    
    # Проверяем существование конфига
    if not os.path.exists(args.config):
        print(f"Ошибка: Конфигурационный файл не найден: {args.config}")
        sys.exit(1)
    
    print("=" * 70)
    print("ETL Pipeline - Модульная версия")
    print("=" * 70)
    print(f"Конфигурация: {args.config}")
    print(f"Файл дампа: {args.dump_file}")
    print(f"Режим: {'Инициализация' if args.init else 'Ночная обработка'}")
    print(f"Очистка временной БД: {'Да' if args.cleanup else 'Нет'}")
    print("=" * 70)
    
    try:
        # Создаем оркестратор задач
        runner = TaskRunner(config_path=args.config)
        
        # Регистрируем задачи в зависимости от режима
        if args.init:
            # Режим инициализации - только восстановление из дампа
            print("\n[РЕЖИМ ИНИЦИАЛИЗАЦИИ]")
            print("Выполняется полная загрузка дампа в основную базу...\n")
            
            # Для инициализации используем упрощенный пайплайн
            runner.add_task(RestoreTask("RestoreToMain", is_init=True))
        else:
            # Режим ночной обработки - полный пайплайн
            print("\n[РЕЖИМ НОЧНОЙ ОБРАБОТКИ]")
            print("Выполняется инкрементальное обновление данных...\n")
            
            # Последовательность задач для ночной обработки
            runner.add_task(ExtractTask("Extract"))
            runner.add_task(RestoreTask("RestoreToTemp"))
            runner.add_task(CompareTask("Compare"))
            runner.add_task(TransformCustomValuesTask("TransformCustomValues"))
            runner.add_task(LoadTask("Load"))
            runner.add_task(WeeklyTask("Weekly"))
            
            if args.cleanup:
                runner.add_task(CleanupTask("Cleanup"))
        
        # Запускаем пайплайн
        success = runner.run(dump_file=args.dump_file)
        
        if success:
            print("\n" + "=" * 70)
            print("ETL Pipeline успешно завершен!")
            print("=" * 70)
            sys.exit(0)
        else:
            print("\n" + "=" * 70)
            print("ETL Pipeline завершен с ошибками!")
            print("=" * 70)
            sys.exit(1)
            
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
