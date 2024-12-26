import jsonlines
import io
from my_types import Row


def get_data_as_tuples_chunked(
    path, limit: int = -1, chunk_size: int = 1
) -> list[tuple[Row]]:
    with open(path, "r") as fp:
        rows = list(jsonlines.Reader(fp))
        all_rows = []
        for i in range(0, len(rows[:limit]), chunk_size):
            all_rows.append([tuple(row.values()) for row in rows[i : i + chunk_size]])
        return all_rows


def get_data_as_tuples(path, limit: int = -1) -> list[tuple[Row]]:
    with open(path, "r") as fp:
        row_reader = jsonlines.Reader(fp)
        all_rows = [tuple(row.values()) for row in list(row_reader)[:limit]]
        return all_rows


def get_data_as_csv(path, limit: int = -1) -> io.StringIO:
    with open(path, "r") as fp:
        row_reader = jsonlines.Reader(fp)
        all_rows = [
            "\t".join([str(item).replace("\t", "\\t") for item in list(row.values())])
            .replace("\\0", "")
            .replace("\\", "\\\\")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            for row in list(row_reader)[:limit]
        ]
        return all_rows
