from dagster import ConfigurableResource
from sqlalchemy import create_engine


class WarehouseResource(ConfigurableResource):
    host: str
    port: int = 5432
    user: str
    password: str
    database: str

    def get_engine(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(url)
