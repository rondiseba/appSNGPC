import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
import numpy as np

def gerar_modelo_arima(df, principio_ativo):
    df_filtro = df[df['principio_ativo'] == principio_ativo].copy()
    df_filtro = df_filtro[df_filtro['ano'].notnull() & df_filtro['mes'].notnull()]
    df_filtro['ano'] = df_filtro['ano'].astype(int)
    df_filtro['mes'] = df_filtro['mes'].astype(int)

    df_filtro = df_filtro.groupby(['ano', 'mes'])['quantidade_vendida'].sum().reset_index()
    df_filtro['data'] = pd.to_datetime({
        'year': df_filtro['ano'],
        'month': df_filtro['mes'],
        'day': 1
    })
    df_filtro.set_index('data', inplace=True)
    serie = df_filtro['quantidade_vendida'].asfreq('MS').fillna(0)

    modelo = ARIMA(serie, order=(1, 1, 1))
    modelo_treinado = modelo.fit()
    previsao = modelo_treinado.forecast(steps=6)
    erro = mean_squared_error(serie[-6:], previsao)

    return previsao, erro
