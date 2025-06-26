# src/aplicacao/clusterizacao.py (VERSÃO ATUALIZADA)
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np # Para np.nan e select_dtypes

def agrupar_prescricoes(df: pd.DataFrame, 
                        metodo: str = "kmeans", 
                        features: list = ['quantidade_vendida', 'idade'], # Default features
                        kmeans_n_clusters: int = 4, 
                        dbscan_eps: float = 0.5, 
                        dbscan_min_samples: int = 5) -> pd.DataFrame:
    """
    Agrupa prescrições usando KMeans ou DBSCAN com parâmetros customizáveis.

    Args:
        df: DataFrame com os dados.
        metodo: 'kmeans' ou 'dbscan'.
        features: Lista de colunas a serem usadas para clusterização.
        kmeans_n_clusters: Número de clusters para KMeans.
        dbscan_eps: Raio da vizinhança para DBSCAN.
        dbscan_min_samples: Número mínimo de amostras para DBSCAN.

    Returns:
        DataFrame original com uma nova coluna 'cluster'.
    """
    df_resultado = df.copy()

    valid_features = [f for f in features if f in df_resultado.columns]
    if not valid_features: # Alterado para verificar se a lista está vazia
        print("Alerta: Nenhuma feature válida selecionada para clusterização.")
        df_resultado['cluster'] = np.nan
        return df_resultado

    dados_para_clusterizar = df_resultado[valid_features].copy()

    for col in valid_features:
        dados_para_clusterizar[col] = pd.to_numeric(dados_para_clusterizar[col], errors='coerce')
    
    dados_para_clusterizar.dropna(subset=valid_features, inplace=True)
    
    if 'idade' in valid_features and 'idade' in dados_para_clusterizar.columns: # Checar se 'idade' ainda existe após conversão/dropna
        if pd.api.types.is_numeric_dtype(dados_para_clusterizar['idade']): # Checar se é numérica antes de filtrar > 0
            dados_para_clusterizar = dados_para_clusterizar[dados_para_clusterizar['idade'] > 0]

    min_amostras_necessarias = 1
    if metodo == "kmeans":
        min_amostras_necessarias = kmeans_n_clusters
    # Para DBSCAN, min_samples é um parâmetro, mas o algoritmo pode rodar com menos,
    # embora os clusters possam não ser significativos. Uma verificação geral de dados é mais útil.
    
    if dados_para_clusterizar.empty or len(dados_para_clusterizar) < 2: # Mínimo de 2 amostras para StandardScaler
        print(f"Alerta: Dados insuficientes ({len(dados_para_clusterizar)} amostras) para clusterização após a filtragem.")
        df_resultado['cluster'] = np.nan 
        return df_resultado
    
    if metodo == "kmeans" and len(dados_para_clusterizar) < kmeans_n_clusters:
         print(f"Alerta: Número de amostras ({len(dados_para_clusterizar)}) é menor que o número de clusters K ({kmeans_n_clusters}).")
         # KMeans não pode ter mais clusters que amostras. Pode-se ajustar kmeans_n_clusters ou retornar erro.
         # Por ora, o KMeans vai falhar ou o scikit-learn pode ajustar internamente/gerar erro.
         # A lógica na página Streamlit já tenta ajustar n_clusters_ajustado.

    scaler = StandardScaler()
    # Tentar fit_transform, mas se houver apenas 1 amostra após filtragens, pode falhar.
    # A verificação len(dados_para_clusterizar) < 2 já deve cobrir isso.
    try:
        dados_padronizados = scaler.fit_transform(dados_para_clusterizar)
    except ValueError as e:
        print(f"Erro na padronização dos dados (StandardScaler): {e}. Pode ser devido a dados insuficientes ou não numéricos restantes.")
        df_resultado['cluster'] = np.nan
        return df_resultado


    labels = []
    if metodo == "kmeans":
        # Ajuste de n_clusters se for maior que o número de amostras disponíveis
        n_clusters_final = min(kmeans_n_clusters, len(dados_padronizados))
        if n_clusters_final < 1: n_clusters_final = 1 # Precisa de pelo menos 1 cluster

        if n_clusters_final != kmeans_n_clusters:
             print(f"Alerta: kmeans_n_clusters foi ajustado de {kmeans_n_clusters} para {n_clusters_final} devido ao número de amostras ({len(dados_padronizados)}).")
        
        try:
            modelo = KMeans(n_clusters=n_clusters_final, random_state=42, n_init='auto')
            labels = modelo.fit_predict(dados_padronizados)
        except Exception as e:
            print(f"Erro ao executar KMeans: {e}")
            df_resultado['cluster'] = np.nan
            return df_resultado

    elif metodo == "dbscan":
        try:
            modelo = DBSCAN(eps=dbscan_eps, min_samples=dbscan_min_samples)
            labels = modelo.fit_predict(dados_padronizados)
        except Exception as e:
            print(f"Erro ao executar DBSCAN: {e}")
            df_resultado['cluster'] = np.nan
            return df_resultado
    else:
        print(f"Erro: Método de clusterização '{metodo}' não suportado.")
        df_resultado['cluster'] = np.nan
        return df_resultado

    df_resultado.loc[dados_para_clusterizar.index, 'cluster'] = labels
    
    return df_resultado