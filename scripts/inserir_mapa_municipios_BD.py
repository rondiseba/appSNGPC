# scripts/ingerir_mapa_municipios.py
import pandas as pd
from pathlib import Path
import sys
import duckdb

# Adiciona a pasta raiz ao caminho do Python
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Importa as constantes e a função de conexão
from src.utils.database_utils import get_db_connection_for_etl, DUCKDB_FILE_PATH

def carregar_mapeamento_municipios_para_db():
    """
    Lê o arquivo CSV de mapeamento de municípios e o salva em uma
    nova tabela no banco de dados DuckDB.
    """
    print("--- INICIANDO CARGA DO MAPEAMENTO DE MUNICÍPIOS PARA O BANCO DE DADOS ---")
    
    # Define a nova constante para a tabela de municípios
    NOME_TABELA_MUNICIPIOS = "mapeamento_municipios"
    
    conexao = None
    try:
        CAMINHO_MUNICIPIOS_CSV = project_root / "dados" / "mapeamento_municipios.csv"
        
        print(f"Lendo arquivo de mapeamento de municípios: {CAMINHO_MUNICIPIOS_CSV}")
        df_mapa_municipios = pd.read_csv(CAMINHO_MUNICIPIOS_CSV, sep=',')
        
        # Padroniza nomes das colunas
        df_mapa_municipios.columns = [col.lower().strip().replace(' ', '_') for col in df_mapa_municipios.columns]
        
        # Garante que o ID do município seja tratado como texto para a junção
        if 'id_municipio' in df_mapa_municipios.columns:
            df_mapa_municipios['id_municipio'] = df_mapa_municipios['id_municipio'].astype(str)
        
        conexao = get_db_connection_for_etl()
        if conexao is None:
            raise ConnectionError("Falha na conexão com o DuckDB.")
            
        print(f"Criando ou substituindo a tabela '{NOME_TABELA_MUNICIPIOS}'...")
        conexao.sql(f"CREATE OR REPLACE TABLE {NOME_TABELA_MUNICIPIOS} AS SELECT * FROM df_mapa_municipios")
        
        total_linhas = conexao.execute(f"SELECT COUNT(*) FROM {NOME_TABELA_MUNICIPIOS};").fetchone()[0]
        print(f"-> Tabela '{NOME_TABELA_MUNICIPIOS}' criada/atualizada com sucesso com {total_linhas} registros.")

    except FileNotFoundError:
        print(f"ERRO: Arquivo de mapeamento não encontrado em {CAMINHO_MUNICIPIOS_CSV}")
    except Exception as e:
        print(f"ERRO na carga do mapeamento de municípios: {e}")
    finally:
        if 'conexao' in locals() and conexao:
            conexao.close()
            print("Conexão com DuckDB fechada.")

if __name__ == '__main__':
    carregar_mapeamento_municipios_para_db()