# diagnostico_dados.py
import pandas as pd
from pathlib import Path

# Caminho para a pasta de dados (ajuste se o script não estiver na raiz do projeto)
pasta_dados = Path("dados")
arquivo_bruto = pasta_dados / "Dados_Brutos_SNGPC.csv"
arquivo_processado = pasta_dados / "dados_processados.csv"

def inspecionar_csv(caminho_arquivo, nome_amigavel="Arquivo"):
    print(f"\n--- Inspecionando: {nome_amigavel} ---")
    print(f"Caminho completo: {caminho_arquivo.resolve()}")

    if not caminho_arquivo.exists():
        print(f"ERRO: Arquivo não encontrado em {caminho_arquivo}")
        return

    try:
        # Tentar ler apenas as primeiras linhas para evitar carregar tudo se for muito grande
        # e para obter os tipos de dados inferidos pelo Pandas nas primeiras linhas.
        df_sample = pd.read_csv(caminho_arquivo, nrows=10)
        print("\nPrimeiras 5 linhas:")
        print(df_sample.head())

        print("\nColunas encontradas (das primeiras 10 linhas):")
        print(list(df_sample.columns))

        print("\nInformações do DataFrame (das primeiras 10 linhas):")
        df_sample.info()

        # Se quiser verificar a existência de uma coluna específica em todo o arquivo (pode ser lento)
        # Para verificar 'nome_municipio' no arquivo processado:
        if nome_amigavel == "Dados Processados":
            print("\nVerificando a presença da coluna 'nome_municipio' (lendo apenas nomes das colunas de todo o arquivo)...")
            # Isso lê apenas o cabeçalho, mais rápido que carregar o DF inteiro
            cols_no_arquivo_completo = pd.read_csv(caminho_arquivo, nrows=0).columns.tolist()
            if 'nome_municipio' in cols_no_arquivo_completo:
                print("==> SUCESSO: A coluna 'nome_municipio' FOI encontrada no cabeçalho do arquivo processado completo.")
            else:
                print("==> ATENÇÃO: A coluna 'nome_municipio' NÃO FOI encontrada no cabeçalho do arquivo processado completo.")
            
            # Se quiser ver os valores da coluna 'nome_municipio' das primeiras linhas
            if 'nome_municipio' in df_sample.columns:
                 print("\nValores da coluna 'nome_municipio' nas primeiras linhas:")
                 print(df_sample['nome_municipio'].value_counts(dropna=False))


    except Exception as e:
        print(f"ERRO ao tentar ler ou inspecionar {caminho_arquivo}: {e}")

if __name__ == "__main__":
    print("Executando diagnóstico dos arquivos CSV...")
    inspecionar_csv(arquivo_bruto, "Dados Brutos (SNGPC)")
    inspecionar_csv(arquivo_processado, "Dados Processados")
    print("\n--- Diagnóstico Concluído ---")