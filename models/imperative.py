from sqlalchemy import (
    MetaData, Table, Column,
    String, Integer, Date, VARCHAR,text,
    insert,update,select,delete,func)
from datetime import datetime as dt
from config.db import engine_postgresql,engine_mysql


# Mysql
def create_tables():
    try:
        metadata = MetaData()

        client_table = Table(
            "clientes",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("nome", VARCHAR(100), nullable=False),
            Column("email", VARCHAR(255), nullable=False, unique=True),
            Column("telefone",VARCHAR(20), nullable=True),
            Column("cidade", VARCHAR(100), nullable=True),
            Column("created_at", Date, nullable=False, default=dt.today().strftime('%Y-%m-%d'))
        )

        metadata.create_all(engine_mysql)
    except Exception as e:
        print(f"Erro ao criar tabelas. \n{e}")

    return {
        "clients": client_table
    }

def insert_table(data: list, table: object):
    try:
        with engine_mysql.connect() as conn:
            stmt = insert(table).values(data)

            result = conn.execute(stmt)
            conn.commit()

            return {
                "operacao":"Sucesso",
                "quantidade_de_registros":result.rowcount
            }
    except Exception as e:
         return {
            "operacao": "Erro",
            "erro": str(e)
        }

def update_table(table:object, data:dict, pk:int):
    try:
        with engine_mysql.connect() as conn:
            stmt = update(table).where(table.c.id == pk).values(data)

            result = conn.execute(stmt)
            conn.commit()

            return {
                "operacao":"sucesso",
                "id alterado":pk,
                "linhas exluidas":result.rowcount
            }
    except Exception as e:
        print(f'Erro {e}')

def delete_table(table:object, pk:int):
    try:
        with engine_mysql.connect() as conn:
            stmt = delete(table).where(table.c.id == pk)

            result = conn.execute(stmt)
            conn.commit()

            return {
                "operacao":"sucesso",
                "id excuido":pk,
                "linhas exluidas":result.rowcount
            }
    except Exception as e:
        print(e)

# postgresql

# Extract
def select_table(table):
    try:
        with engine_postgresql.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table}"))

            resultado = result.all()

            return resultado
    except Exception as e:
        print(e)

