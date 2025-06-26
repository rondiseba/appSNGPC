# Envio de mapeamento de princípios ativos controlados para o banco de dados DuckDB
import pandas as pd
import duckdb
from pathlib import Path
from unidecode import unidecode # Importar a nova biblioteca

def carregar_mapeamento_para_db():
    print("--- INICIANDO CARGA DO MAPEAMENTO PARA O BANCO DE DADOS ---")
    
    BASE_DIR = Path(__file__).resolve().parent.parent
    CAMINHO_MAPEAMENTO_CSV = BASE_DIR / "dados" / "mapeamento_Controlados.csv"
    DUCKDB_FILE_PATH = BASE_DIR / "dados" / "sngpc_analytics.duckdb"
    NOME_TABELA = "mapeamento_controlados"

    try:
        print(f"Lendo arquivo de mapeamento: {CAMINHO_MAPEAMENTO_CSV}")
        df_mapeamento = pd.read_csv(CAMINHO_MAPEAMENTO_CSV, sep=',')
        
        print("Padronizando nomes das colunas...")
        df_mapeamento.columns = [col.lower().strip().replace(' ', '_') for col in df_mapeamento.columns]
        
        if 'principio_ativo' in df_mapeamento.columns:
            print("Padronizando e criando chave de junção sem acentos...")
            # Limpa e padroniza o nome do princípio ativo
            df_mapeamento['principio_ativo_base'] = df_mapeamento['principio_ativo'].str.strip().str.upper()
            # Cria uma chave de junção limpa, sem acentos e em maiúsculas
            df_mapeamento['join_key'] = df_mapeamento['principio_ativo_base'].apply(lambda x: unidecode(str(x)).upper())

        conexao = duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=False)
        
        print(f"Criando ou substituindo a tabela '{NOME_TABELA}'...")
        conexao.sql(f"CREATE OR REPLACE TABLE {NOME_TABELA} AS SELECT * FROM df_mapeamento")
        
        total_linhas = conexao.execute(f"SELECT COUNT(*) FROM {NOME_TABELA};").fetchone()[0]
        print(f"Tabela '{NOME_TABELA}' criada/atualizada com sucesso com {total_linhas} registros.")

    except Exception as e:
        print(f"ERRO durante a ingestão do mapeamento: {e}")
    finally:
        if 'conexao' in locals():
            conexao.close()
            print("Conexão com DuckDB fechada.")

if __name__ == '__main__':
    carregar_mapeamento_para_db()