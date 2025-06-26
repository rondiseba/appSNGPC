# src/infra/repositorio_dados.py
import pandas as pd
from pathlib import Path
import duckdb # Adicionar import

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DUCKDB_FILE_PATH = BASE_DIR / "dados" / "sngpc_analytics.duckdb" # Caminho para o arquivo DuckDB
TABLE_NAME = "prescricoes" # Nome da tabela que você usou no script de ingestão

def carregar_dados_processados_sngpc(): # Nome da função mantido para compatibilidade
    """
    Carrega os dados processados (anos 2019 e 2020) do banco de dados DuckDB.
    """
    if not DUCKDB_FILE_PATH.exists():
        # Este erro deve idealmente ser uma exceção mais específica ou tratada de forma mais robusta.
        # Para agora, vamos levantar um FileNotFoundError para ser pego no app.py
        raise FileNotFoundError(
            f"Arquivo de banco de dados DuckDB não encontrado em {DUCKDB_FILE_PATH}. "
            "Execute o script de ingestão (ex: ingest_to_duckdb.py) primeiro."
        )

    print(f"Conectando ao DuckDB em: {DUCKDB_FILE_PATH}")
    con = None # Inicializa con para garantir que exista no bloco finally
    try:
        con = duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=True) # Abrir em modo read-only para consultas
        
        # A filtragem por ano agora é feita diretamente na consulta SQL!
        query = f"SELECT * FROM {TABLE_NAME} WHERE ano IN (2019, 2020);"
        
        print(f"Executando consulta: {query}")
        # df = con.execute(query).fetchdf() # fetchdf() retorna um DataFrame pandas
        
        # Para datasets muito grandes, mesmo após o filtro WHERE,
        # se o resultado ainda for massivo para a memória do Pandas,
        # pode-se considerar processamento em chunks ou consultas mais agregadas.
        # Mas para 2 anos de dados (se o total é 21M para vários anos), isso já deve ser bem menor.
        
        # Usar .arrow() e depois .to_pandas() pode ser mais eficiente para conversão
        arrow_table = con.execute(query).fetch_arrow_table()
        df = arrow_table.to_pandas()
        
        print(f"Dados carregados do DuckDB e convertidos para DataFrame pandas: {len(df):,} linhas.")
        
        if df.empty:
            # Isso pode acontecer se a tabela não tiver dados para 2019 e 2020.
            print(f"Atenção: Nenhum dado retornado do DuckDB para os anos 2019 e 2020 da tabela '{TABLE_NAME}'.")
            # O app.py já tem lógica para lidar com df vazio.

        return df
    except Exception as e:
        print(f"Erro ao carregar dados do DuckDB: {e}")
        # Re-levantar a exceção para que app.py possa tratá-la ou mostrar um erro mais genérico.
        raise 
    finally:
        if con:
            print("Fechando conexão com DuckDB.")
            con.close()

# A função carregar_dados_brutos_sngpc() pode ser mantida se você ainda a usa para algo,
# ou removida se não for mais necessária.
# def carregar_dados_brutos_sngpc():
#     caminho_csv = BASE_DIR / "dados" / "Dados_Brutos_SNGPC.csv"
#     df = pd.read_csv(caminho_csv) 
#     return df