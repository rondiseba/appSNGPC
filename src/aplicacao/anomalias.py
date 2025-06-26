from sklearn.ensemble import IsolationForest
import pandas as pd

def detectar_anomalias_isolation_forest(df):
    df_limpo = df[['quantidade_vendida', 'idade']].dropna()
    modelo = IsolationForest(contamination=0.05, random_state=42)
    df_limpo['anomaly'] = modelo.fit_predict(df_limpo)

    df_resultado = df.copy()
    df_resultado['anomaly'] = -1
    df_resultado.loc[df_limpo.index, 'anomaly'] = df_limpo['anomaly']

    return df_resultado[df_resultado['anomaly'] == -1]