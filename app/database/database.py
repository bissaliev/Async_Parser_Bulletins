from typing import Any, Callable

from config import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# DATABASE_URL = settings.get_db_postgres_url()
DATABASE_URL = settings.get_db_sqlite_url()

engine = create_engine(DATABASE_URL)


class BaseModel(DeclarativeBase): ...


SessionLocal = sessionmaker(autoflush=False, bind=engine)


BaseModel.metadata.create_all(bind=engine)


def context_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """Декоратор для управления сессией SQLAlchemy"""

    def wrapper(*args, **kwargs):
        session = SessionLocal()
        try:
            result = func(session, *args, **kwargs)
            return result
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    return wrapper
