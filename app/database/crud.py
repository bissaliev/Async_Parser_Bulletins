from database.database import context_session
from database.models import SpimexTradingResults
from sqlalchemy import insert
from sqlalchemy.orm import Session


@context_session
def mass_create_trade(session: Session, lst_data: list[dict]) -> None:
    """Выполняет массовую вставку данных в таблицу `SpimexTradingResults`"""
    session.execute(insert(SpimexTradingResults), lst_data)
    session.commit()
