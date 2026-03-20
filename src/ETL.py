from models.imperative import insert_table, create_tables, select_table
import json
import pandas as pd

dict_table_name = create_tables()

def read_json() -> list:
    with open(r"data\clients.json", "r", encoding="utf-8") as file:
        arquivo = json.load(file) # [{"nome":"...","email":"..."},{...}]

        return arquivo
     
def remover_nulos_e_duplicados(dados:list) -> dict:
    """
    Como a função diz, remove nulos e duplicados usando métodos da classe
    dataframe do pandas.

    - Parâmetros: dados(lista) -> uma lista de dicionários

    - return {
        "dados": LIST(DICT),
        "email_nulos": INT,
        "nome_nulos": INT,
        "duplicados": INT
    }
    
    """

    dataframe = pd.DataFrame(dados)
    dataframe_sem_nomes_nulos = dataframe.dropna(subset=["nome"])
    nomes_nulos = (len(dataframe) - len(dataframe_sem_nomes_nulos))

    dataframe_sem_emails_nulos = dataframe_sem_nomes_nulos.dropna(subset=["email"])
    email_nulos = (len(dataframe_sem_nomes_nulos) - len(dataframe_sem_emails_nulos))
    dataframe_sem_duplicados = dataframe_sem_emails_nulos.drop_duplicates(subset=["email"])
    duplicados = (len(dataframe_sem_emails_nulos)-len(dataframe_sem_duplicados))

    return {
        "dados":dataframe_sem_duplicados.to_dict(orient="records"),
        "email_nulos":email_nulos,
        "nome_nulos":nomes_nulos,
        "duplicados":duplicados

    }

def _limpar_nan(dados:list):
    """
    função auxiliar que limpa os dados NaN do Pandas e transforma em None

    - parâmetros: dados(lista) -> lista de dicionários

    - return dados (lista de dicionários)
    """
    for item in dados:
        for k,v in item.items():
            if pd.isna(v):
                item[k] = None
    return dados


def normalizar(dados:list) -> dict:
    """
    `Normaliza dados usando funções auxiliares como _format_name e _verify_email
    nomes são formatados se não forem none e email errados não saõ incluídos`

    - parâmetros: dados(lista) -> lista de dicionários

    - return {
        "dados_normalizados": lista de dicionários,
        "dados_email_invalido": lista de dicionários
    }

    """

    normalizado = []
    email_invalido = []

    for index, value in enumerate(dados): # Separando os dicts i = {}
        value["nome"] = _format_name(value["nome"])

        if _verify_email(value["email"]) == False:
            email_invalido.append(value)
            continue

        normalizado.append(value)
    
    return {
        "dados_normalizados": _limpar_nan(normalizado),
        "dados_email_invalido": email_invalido
    }


def _verify_email(email:str) -> bool:
    """
    função que verifica um email a partir de uma string

    - parâmetros: email(str) 

    - return: Boolean (True/False)
    """
    if "@" not in email or ".com" not in email or "email" not in email:
        return False
    return True

def _format_name(name:str) -> str:
    nome = " ".join(name.lower().split())
    return nome

def _write_json(dados:list) -> dict:
    try:
        with open(r"data\clients.json", 'w', encoding="utf-8") as file:
            json.dump(dados, file,indent=4, ensure_ascii=False)
        return {
            "status":"sucesso"
        }
    except Exception as e:
        return {
            "status": "erro",
            "erro": e
        }

# ================================================
# Funções principais ETL

def extract(table:str="clients"):
    resultado = select_table(table)

    formatado = []
    for i in resultado:
        formatado.append({
            "id":i[0],
            "nome":i[1],
            "email":i[2],
            "telefone":i[3],
            "cidade":i[4],
            "created_at":i[5]
        })

    _write_json(formatado)

    return {
        "dados":formatado,
        "registros_extraidos":len(formatado)
    }

def transform() ->dict:
    dados_lidos = read_json()
    dados_nao_nulos_e_duplicados = remover_nulos_e_duplicados(dados_lidos)
    dados_normalizados = normalizar(dados_nao_nulos_e_duplicados["dados"])

    return {
        "dados":dados_normalizados["dados_normalizados"],
        "analise":{
            "registros_extraidos":len(dados_lidos),
            "registros_validos":len(dados_normalizados["dados_normalizados"]),
            "registros_descartados":{
                "quantidade_total": (len(dados_lidos) - len(dados_normalizados["dados_normalizados"])),
                "motivos":{
                    "email_invalido":(dados_nao_nulos_e_duplicados["email_nulos"]+len(dados_normalizados["dados_email_invalido"])),
                    "nome_nulo":dados_nao_nulos_e_duplicados["nome_nulos"],
                    "duplicados":dados_nao_nulos_e_duplicados["duplicados"]
                }
                                     
            },
            
        }
        
    }

def load() -> dict:
    """
    return {
        "dados_inseridos": insert,
        "analise": dados["analise"]
    }
    """
    dados = transform()
    insert = insert_table(dados["dados"],dict_table_name["clients"])

    return {
        "dados_inseridos": insert,
        "analise": dados["analise"]
    }

def ETL():
    """
    return {
            "status":"sucesso",
            "analise":dados["analise"]
        }
    """
    try:
        extract()
        dados = load()

        return {
            "status":"sucesso",
            "analise":dados["analise"],
            "dados_inseridos":dados["dados_inseridos"]
        }
    except Exception as e:
        return {
            "status":"Erro",
            "erro": str(e)
        }

