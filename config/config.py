from pydantic_settings import BaseSettings, SettingsConfigDict

class ConfigPostgresql(BaseSettings):
    db_host : str
    db_user : str
    db_password : str
    db_port : str
    db_name: str

    model_config = SettingsConfigDict(
        env_prefix="postgresql_",
        env_file='.env',
        extra="ignore")

class ConfigMysql(BaseSettings):
    db_host: str
    db_user: str
    db_password: str
    db_port: str
    db_name: str

    model_config = SettingsConfigDict(
        env_prefix="mysql_",
        env_file='.env',
        extra="ignore")

config_mysql = ConfigMysql()
config_postgresql = ConfigPostgresql()