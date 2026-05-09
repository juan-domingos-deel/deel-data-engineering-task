from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def init_pool(dsn: str, min_size: int = 1, max_size: int = 4) -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(conninfo=dsn, min_size=min_size, max_size=max_size, open=True)
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    """Yield a connection inside a single transaction; rollback on error."""
    assert _pool is not None, "init_pool() must be called first"
    with _pool.connection() as conn:
        with conn.transaction():
            yield conn
