import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from src.aplicacao.analise_eda import gerar_relatorio_qualidade

if __name__ == "__main__":
    # Corrige o caminho do arquivo usando os.path.join
    caminho = os.path.join("dados", "dados_processados.csv")
    df = pd.read_csv(caminho)
    gerar_relatorio_qualidade(df)
