def get_script_content(path: str) -> str:
    with open(path, 'r', encoding='utf8') as file:
        return file.read()
