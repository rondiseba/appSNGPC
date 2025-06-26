import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import os
from src.infra.repositorio_dados import carregar_dados_sngpc
from src.aplicacao.anomalias import detectar_anomalias_isolation_forest

def executar_anomalias():
    df = carregar_dados_sngpc()
    
if __name__ == "__main__":
    df = pd.read_csv(os.path.join("dados", "dados_processados.csv"))
    anomalias = detectar_anomalias_isolation_forest(df)
    anomalias.to_csv(os.path.join("dados", "anomalias_detectadas.csv"), index=False)
    print("Anomalias detectadas salvas em dados/anomalias_detectadas.csv")