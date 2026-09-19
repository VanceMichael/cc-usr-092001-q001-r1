"""初始化项目的本地 SQLite 数据文件。"""

import os
import sqlite3
from pathlib import Path


def main() -> None:
    database_path = Path(os.environ.get("DATABASE_PATH", "data/app.sqlite3"))
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS service_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT OR IGNORE INTO service_meta(key, value) VALUES('schema_version', '1')"
        )
    print(f"数据库初始化完成：{database_path}")


if __name__ == "__main__":
    main()
