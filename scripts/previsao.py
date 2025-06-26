# Esse script é responsável por carregar os dados do SNGPC e salvar em um arquivo CSV.
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.infra.repositorio_dados import carregar_dados_sngpc

def executar_previsao():
    df = carregar_dados_sngpc()

if __name__ == "__main__":
    caminho_json = os.path.join("dados", "dados_SNGPC.json")
    df = carregar_dados_sngpc(caminho_json)
    caminho_saida = os.path.join("dados", "dados_processados.csv")
    df.to_csv(caminho_saida, index=False)
    print("Dados salvos como dados/dados_processados.csv")
