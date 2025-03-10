from database.database import BaseModel, engine
from database.models import SpimexTradingResults  # noqa: F401

print(BaseModel.metadata.tables.keys())

BaseModel.metadata.create_all(bind=engine)
