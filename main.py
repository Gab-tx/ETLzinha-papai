from models.imperative import insert_table, create_tables, select_table
from src.ETL import *
from pprint import pprint
    
if __name__ == "__main__":
    # write_json(normalizar())
    etl = ETL()

    frontend = f"""
=========================================
    Registros Extraídos: {etl["analise"]["registros_extraidos"]}
    Registros Válidos: {etl['analise']['registros_validos']}
    Registros Descartados: {etl['analise']['registros_descartados']['quantidade_total']}

    Motivos:
      - Email Inválido: {etl['analise']['registros_descartados']['motivos']['email_invalido']}
      - Nome Nulo: {etl['analise']['registros_descartados']['motivos']['nome_nulo']}
      - Duplicados: {etl['analise']['registros_descartados']['motivos']['duplicados']}

    Dados Inseridos:
    {etl['dados_inseridos']}
"""

    print(frontend)
    