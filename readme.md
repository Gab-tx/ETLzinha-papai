# Projeto ETL --- Limpeza e Migração de Dados

Projeto desenvolvido como parte do desafio prático da disciplina de
Desenvolvimento de Sistemas.

O objetivo do projeto é construir um **pipeline ETL (Extract, Transform,
Load)** capaz de extrair dados inconsistentes de um banco PostgreSQL
hospedado no Supabase, realizar a limpeza e padronização dos dados e
carregá-los em um banco MySQL local.

------------------------------------------------------------------------

# 📊 Contexto do Problema

Uma empresa possui um sistema legado que armazenava dados de clientes de
forma inadequada. Esses dados foram migrados para um banco PostgreSQL
(Supabase), porém apresentam diversos problemas como:

-   registros duplicados
-   campos nulos
-   emails inválidos
-   nomes com formatação inconsistente

O desafio consiste em desenvolver um pipeline que **corrija esses
problemas e armazene os dados tratados em um novo banco de dados
estruturado**.

------------------------------------------------------------------------

# ⚙️ Arquitetura do Projeto

Pipeline ETL implementado:

Supabase (PostgreSQL)\
↓\
Extract\
↓\
Transform (limpeza e validação)\
↓\
Load\
↓\
MySQL Local

------------------------------------------------------------------------

# 🧩 Tecnologias Utilizadas

-   Python
-   PostgreSQL (Supabase)
-   MySQL
-   SQL

Bibliotecas Python sugeridas:

-   psycopg2 ou sqlalchemy
-   mysql-connector
-   json
-   re (regex)

------------------------------------------------------------------------

# 📂 Estrutura do Projeto

    project-etl/
    │
    ├── src/
    │   ├── extract.py
    │   ├── transform.py
    │   ├── load.py
    │   └── main.py
    │
    ├── config/
    │   └── database.py
    │
    ├── logs/
    │
    ├── requirements.txt
    │
    └── README.md

------------------------------------------------------------------------

# 🔍 Processo ETL

## 1️⃣ Extract

Os dados são extraídos da tabela:

    raw_clients

Banco de dados hospedado no **Supabase (PostgreSQL)**.

Exemplo de consulta utilizada:

``` sql
SELECT * FROM raw_clients;
```

------------------------------------------------------------------------

# 🔧 Transform

Durante a transformação foram aplicadas as seguintes regras:

### Remoção de registros inválidos

-   registros sem email
-   registros sem nome

### Validação de email

Emails foram validados utilizando **validação básica**.


### Padronização de nomes

-   remoção de espaços extras
-   conversão para **Minúsculo**

Exemplo:

    "  ANA silva  " → "ana silva"

### Remoção de duplicados

Registros duplicados foram identificados pelo campo:

    noem e email

------------------------------------------------------------------------

# 📥 Load

Após a limpeza, os dados foram carregados em uma tabela **MySQL** local.

Estrutura da tabela:

``` sql
CREATE TABLE clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    telefone VARCHAR(20),
    cidade VARCHAR(100),
    created_at DATETIME
);
```

------------------------------------------------------------------------

# 📊 Resultados da Limpeza

  Métrica                 Quantidade
  ----------------------- ------------
  Registros extraídos     200
  Registros válidos       34
  Registros descartados   166
  Emails inválidos        104

------------------------------------------------------------------------

# ▶️ Como Executar o Projeto

### 1️⃣ Instalar dependências

    pip install -r requirements.txt

### 2️⃣ Configurar credenciais

Editar o arquivo:

    config/database.py

Adicionando as credenciais fornecidas pelo professor.

### 3️⃣ Executar o pipeline

    python main.py

------------------------------------------------------------------------

# 🧠 Aprendizados

Durante o desenvolvimento deste projeto foram praticados conceitos
importantes de engenharia de dados:

-   Pipeline ETL
-   Integração entre bancos de dados
-   Qualidade de dados
-   Validação e normalização de dados
-   Automação de processos de dados

------------------------------------------------------------------------

# 👨‍💻 Autor

Aluno: **\[Gabriel Gomes\]**

Curso: **Desenvolvimento de Sistemas**

Instituição: **Senac**

Professor: **Davi Saldanha**

------------------------------------------------------------------------

# 📌 Observações

Este projeto foi desenvolvido para fins educacionais com o objetivo de
simular um cenário real de **engenharia de dados envolvendo limpeza e
migração de dados entre sistemas**.

------------------------------------------------------------------------

# 📊 Data Quality Report (Opcional)

  Problema               Quantidade
  ---------------------- ------------
  Registros analisados   200
  Duplicados             47
  Emails inválidos       104
  Campos nulos           15