# Este script é responsável por carregar os dados processados e realizar a clusterização das prescrições médicas.
# Ele utiliza o algoritmo KMeans para agrupar prescrições com base em características como quantidade vend
import sys
import os
import pandas as pd
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


if __name__ == "__main__":
    caminho = os.path.join("dados", "dados_processados.csv")
    df = pd.read_csv(caminho)
    print("Colunas disponíveis:", df.columns.tolist())
    print(df.head())

    # Converte as colunas necessárias para numérico
    df['quantidade_vendida'] = pd.to_numeric(df['quantidade_vendida'], errors='coerce')
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce')

    from src.aplicacao.clusterizacao import agrupar_prescricoes
    df_clusterizado = agrupar_prescricoes(df, metodo="kmeans")
    print(df_clusterizado.head())
