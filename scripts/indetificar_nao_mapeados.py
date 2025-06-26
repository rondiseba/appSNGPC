# Script para identificar princípios ativos não mapeados no banco de dados DuckDB
import duckdb
import pandas as pd
from pathlib import Path
import sys

# Adiciona a pasta raiz ao caminho do Python
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.utils.database_utils import DUCKDB_FILE_PATH, TABLE_NAME

def extrair_ativos_nao_mapeados():
    """
    Conecta ao banco de dados, identifica os princípios ativos que não foram
    mapeados para uma lista da ANVISA e os salva em um arquivo CSV para revisão.
    """
    print("--- INICIANDO IDENTIFICAÇÃO DE ATIVOS NÃO MAPEADOS ---")
    
    conexao = None
    try:
        # Caminho para o novo arquivo CSV de saída
        caminho_saida = project_root / "dados" / "mapeamento_BD_Null.csv"

        conexao = duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=True)
        print(f"Conexão estabelecida com o banco: {DUCKDB_FILE_PATH}")
        
        # Query SQL para encontrar os princípios ativos únicos onde 'anvisa_lista' é nulo ou 'Não Mapeado'
        # Isso captura tanto as falhas de junção quanto os casos explicitamente não mapeados.
        query = f"""
        SELECT DISTINCT principio_ativo
        FROM {TABLE_NAME}
        WHERE anvisa_lista IS NULL OR anvisa_lista = 'Não Mapeado';
        """
        
        print("Executando query para encontrar ativos não mapeados...")
        df_nao_mapeados = conexao.execute(query).fetchdf()
        
        if df_nao_mapeados.empty:
            print("\nÓtima notícia! Nenhum princípio ativo não mapeado foi encontrado.")
            print("Seu arquivo de mapeamento parece estar completo.")
        else:
            total_encontrado = len(df_nao_mapeados)
            print(f"-> Encontrados {total_encontrado} princípios ativos que precisam de revisão.")
            
            # Salva a lista em um novo arquivo CSV para sua curadoria
            df_nao_mapeados.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
            print(f"\nArquivo '{caminho_saida.name}' salvo com sucesso na pasta 'dados/'.")
            print("SUA AÇÃO: Abra este arquivo e use-o como guia para atualizar o 'mapeamento_Controlados.csv'.")

    except Exception as e:
        print(f"\nERRO durante a identificação de ativos não mapeados: {e}")
    finally:
        if 'conexao' in locals() and conexao:
            conexao.close()
            print("\nConexão com DuckDB fechada.")

if __name__ == '__main__':
    extrair_ativos_nao_mapeados()