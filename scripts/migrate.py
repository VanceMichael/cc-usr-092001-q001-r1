"""初始化项目的本地 SQLite 数据文件。"""

import os
from pathlib import Path

from src.storage import SCHEMA_VERSION, initialize_database


def main() -> None:
    database_path = Path(os.environ.get("DATABASE_PATH", "data/app.sqlite3"))
    initialize_database(database_path)
    print(f"数据库初始化完成（schema v{SCHEMA_VERSION}）：{database_path}")


if __name__ == "__main__":
    main()
