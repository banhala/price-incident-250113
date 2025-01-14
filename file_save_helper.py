class FileSaveHelper:
    @classmethod
    def save(cls, data: bytes, filepath: str) -> None:
        with open(filepath, 'wb') as f:
            f.write(data)

    @classmethod
    def read(cls, filepath: str) -> bytes:
        with open(filepath, 'rb') as f:
            return f.read()
