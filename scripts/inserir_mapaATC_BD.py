# inserir_mapaATC_BD.py (VERSÃO CORRIGIDA)

import duckdb
import pandas as pd
from pathlib import Path

def carregar_mapeamento_atc():
    """Carrega o arquivo CSV de mapeamento ATC para uma tabela no DuckDB."""
    BASE_DIR = Path(__file__).resolve().parent.parent
    CAMINHO_CSV = BASE_DIR / "dados" / "mapeamento_atc.csv"
    DUCKDB_FILE_PATH = BASE_DIR / "dados" / "sngpc_analytics.duckdb"
    NOME_TABELA = "mapeamento_atc"
    conexao = None

    print(f"--- INICIANDO CARGA DO MAPEAMENTO ATC PARA O BD ---")
    try:
        conexao = duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=False)
        
        if not CAMINHO_CSV.exists():
            raise FileNotFoundError(f"Arquivo de mapeamento não encontrado: {CAMINHO_CSV}")
            
        print(f"Lendo arquivo de mapeamento: {CAMINHO_CSV}")
        df_atc = pd.read_csv(CAMINHO_CSV, sep=',')
        
        # Apenas padroniza nomes das colunas, sem criar join_key
        df_atc.columns = [col.lower().strip().replace(' ', '_') for col in df_atc.columns]

        print(f"Criando ou substituindo a tabela '{NOME_TABELA}' com dados brutos...")
        conexao.sql(f"CREATE OR REPLACE TABLE {NOME_TABELA} AS SELECT * FROM df_atc")
        
        total = conexao.execute(f"SELECT COUNT(*) FROM {NOME_TABELA}").fetchone()[0]
        print(f"-> Tabela '{NOME_TABELA}' criada com {total} registros.")

    except Exception as e:
        print(f"ERRO na carga do mapeamento ATC: {e}")
    finally:
        if conexao:
            conexao.close()
            print("Conexão com DuckDB fechada.")

if __name__ == '__main__':
    carregar_mapeamento_atc()