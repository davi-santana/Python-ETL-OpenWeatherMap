# 🚀 Python-ETL-OpenWeatherMap

## Visão Geral

Este projeto demonstra a construção de um pipeline ETL (Extração, Transformação e Carga) completo e orquestrado, focado na coleta e processamento de dados de clima em tempo quase real. O objetivo é criar um Data Warehouse simples para consultas de BI.

**Habilidades Chave Demonstradas:**
* Construção de pipeline ETL modular em Python.
* Consumo de API REST e tratamento de JSON.
* Normalização de dados aninhados (Pandas).
* Tratamento de erros de tipagem e chaves ausentes (`DateParseError`, `KeyError`).
* Carregamento em banco de dados relacional (SQLite/SQLAlchemy).
* Configuração de ambiente seguro via `.env` e `.gitignore`.

## 🎯 Tecnologias Utilizadas

| Categoria | Ferramenta | Função no Pipeline |
| :--- | :--- | :--- |
| **Linguagem** | Python 3.10+ | Lógica de programação e orquestração. |
| **Extração (E)** | `requests`, `python-dotenv` | Coleta de dados da API OpenWeatherMap. |
| **Transformação (T)** | `pandas` | Limpeza de dados, normalização de JSON e conversão de tipos de dados. |
| **Carga (L)** | `SQLAlchemy`, `sqlite3` | Conexão e carregamento de dados em Data Warehouse (DW) SQLite. |
| **Orquestração** | Modular (Airflow Ready) | Script `run_pipeline.py` para execução sequencial (E → T → L). |

## 📐 Arquitetura do Pipeline

O pipeline processa os dados em três etapas sequenciais:

1.  **Extração (E):** O script `src/extract/api_extractor.py` coleta os dados de Clima Atual e Previsão para cidades globais. O JSON bruto é salvo em `data/raw/`.
2.  **Transformação (T):** O script `src/transform/data_transformer.py` lê o JSON, achata os campos aninhados, garante o formato correto de datas (`%Y%m%d_%H%M%S`) e salva os dados limpos em CSVs separados (`current_weather` e `forecast_weather`) na pasta `data/cleaned/`.
3.  **Carga (L):** O script `src/load/data_loader.py` insere os dados dos CSVs limpos nas tabelas `current_weather` e `forecast_weather` no arquivo `data/weather_data_warehouse.db`.



## 📋 Como Rodar o Projeto

### 1. Pré-requisitos e Instalação

1.  Clone o repositório:
    ```bash
    git clone [SEU_LINK_DO_REPOSITORIO]
    cd Python-ETL-OpenWeatherMap
    ```
2.  Crie e ative um ambiente virtual:
    ```bash
    python -m venv venv
    source venv/bin/activate # Linux/macOS
    # .\venv\Scripts\activate # Windows
    ```
3.  Instale as dependências:
    ```bash
    pip install requests python-dotenv pandas sqlalchemy
    # Ou use: pip install -r requirements.txt
    ```

### 2. Configuração de Segurança

1.  **Crie o Arquivo `.env`** na raiz do projeto e insira sua chave de API (AppID):
    ```ini
    # .env
    OPENWEATHER_API_KEY="SUA_CHAVE_DE_32_CARACTERES_AQUI"
    ```
2.  **Configure o `.gitignore`** para garantir que a chave não seja versionada:
    ```gitignore
    # .gitignore (Certifique-se que estas linhas estão presentes)
    .env
    venv/
    *.db
    ```

### 3. Execução

Rode o script de orquestração principal na raiz do projeto:

```bash
python run_pipeline.py

