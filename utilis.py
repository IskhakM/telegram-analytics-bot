from datetime import datetime, timezone


def get_naive_utc(dt_str):
    """Преобразует строку ISO в datetime, привязывает к UTC,"""
    """а затем делает наивным (без tzinfo)."""

    dt_aware = datetime.fromisoformat(dt_str)
    dt_naive_utc = dt_aware.astimezone(timezone.utc).replace(tzinfo=None)

    return dt_naive_utc


async def run_sql_script(conn, filename):
    """Выполняет SQL-скрипт из файла."""
    print(f'Выполнение скрипта {filename}...')
    with open(filename, 'r', encoding='utf-8') as f:
        await conn.execute(f.read())
    print(f'Скрипт {filename} выполнен.')
