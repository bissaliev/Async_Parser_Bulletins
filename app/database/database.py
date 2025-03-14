from functools import wraps
from typing import Any, Callable

from config import settings
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

# DATABASE_URL = settings.get_db_postgres_url()
DATABASE_URL = settings.get_db_sqlite_url()

engine = create_async_engine(DATABASE_URL)


class BaseModel(AsyncAttrs, DeclarativeBase):
    __abstract__ = True


AsyncSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


def async_context_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """Декоратор для управления сессией SQLAlchemy"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with AsyncSessionLocal() as session:
            try:
                result = await func(session, *args, **kwargs)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                raise e

    return wrapper
