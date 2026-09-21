"""初始化项目的本地 SQLite 数据文件。"""

import os
from pathlib import Path

from src.store import Store


def main() -> None:
    database_path = Path(os.environ.get("DATABASE_PATH", "data/app.sqlite3"))
    store = Store(database_path)
    with store.transaction() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO service_meta(key, value) VALUES('schema_version', '1')"
        )
    store.close()
    print(f"数据库初始化完成：{database_path}")


if __name__ == "__main__":
    main()
