import hashlib
import json


def append_hash_to_data(data: list) -> list:
    """Добавление хешей данных каждой строки в данные."""
    return [{**row, '__row_hash': make_hash_from_row_values(row)} for row in data]


def make_hash_from_row_values(row: dict) -> str:
    """Хеш по значениям строки."""
    return hashlib.sha512(''.join(str(v) for v in row.values()).encode()).hexdigest()


def dump_json_values(data: list) -> list:
    """Преобразование данных типов list и dict в строки."""
    results = []
    for row in data[:]:
        converted_row = {}
        for k, v in row.items():
            if isinstance(v, list) or isinstance(v, dict):
                converted_row[k] = json.dumps(v, default=str, ensure_ascii=False)
            else:
                converted_row[k] = v
        results.append(converted_row)

    return results
