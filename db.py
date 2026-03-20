from sqlalchemy import create_engine
from config import config_mysql,config_postgresql

try:
    engine_mysql = create_engine(
        f"mysql://{config_mysql.db_user}:{config_mysql.db_password}@{config_mysql.db_host}:{config_mysql.db_port}/{config_mysql.db_name}"
    )
    engine_postgresql = create_engine(
        f"postgresql+psycopg2://{config_postgresql.db_user}:{config_postgresql.db_password}@{config_postgresql.db_host}:{config_postgresql.db_port}/{config_postgresql.db_name}"
    )
except Exception as e:
    print(f"Erro ao conectar com o banco de dados: {e}")