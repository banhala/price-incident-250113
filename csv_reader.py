class CsvReader:
    @classmethod
    def read(cls, filepath: str, limit: int | None) -> list[str]:
        with open(filepath) as f:
            print('filepath: ', filepath)
            lines = f.readlines()
            if limit is None:
                return lines[1:]
            return lines[1:limit + 1]
