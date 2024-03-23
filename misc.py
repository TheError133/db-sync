import hashlib


def append_hash_to_data(data: list) -> list:
    """Добавление хешей данных каждой строки в данные."""
    return [{**row, '__row_hash': make_hash_from_row_values(row)} for row in data]


def make_hash_from_row_values(row: dict) -> str:
    """Хеш по значениям строки."""
    return hashlib.sha512(''.join(str(v) for v in row.values()).encode()).hexdigest()
