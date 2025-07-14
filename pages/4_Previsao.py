# Pagina para previsão de séries temporais usando ARIMA
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from statsmodels.tsa.arima.model import ARIMA
import warnings

# --- Novas Importações dos Módulos de Utilitários ---
from src.utils.database_utils import get_duckdb_connection, carregar_opcoes_previsao, TABLE_NAME

# Ignorar avisos comuns do statsmodels sobre convergência, etc.
warnings.filterwarnings("ignore")

# --- Funções Específicas da Página (Busca de dados e Modelagem) ---
@st.cache_data(show_spinner="Preparando série temporal a partir do banco de dados...")
def fetch_timeseries_data(filtro_pa, filtro_mun, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return pd.DataFrame()

    conditions = ["ano IN (2019, 2020)", "data IS NOT NULL", "quantidade_vendida IS NOT NULL"]
    params = []
    
    if filtro_pa != "Total Geral":
        conditions.append("principio_ativo = ?")
        params.append(filtro_pa)
    if filtro_mun != "Total Geral":
        conditions.append("nome_municipio = ?")
        params.append(filtro_mun)
        
    where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT 
            strftime(data, '%Y-%m-01') AS mes,
            SUM(quantidade_vendida) AS valor
        FROM {tabela}
        {where_clause}
        GROUP BY 1
        ORDER BY 1;
    """
    try:
        ts_df = conn.execute(query, params).fetchdf()
        if ts_df.empty:
            return pd.DataFrame()
        
        ts_df['mes'] = pd.to_datetime(ts_df['mes'])
        ts_df = ts_df.set_index('mes')
        
        date_range = pd.date_range(start=ts_df.index.min(), end=ts_df.index.max(), freq='MS')
        ts_df = ts_df.reindex(date_range, fill_value=0)
        
        return ts_df

    except Exception as e:
        st.error(f"Erro ao buscar série temporal: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner="Treinando modelo ARIMA e gerando previsão...")
def run_arima_forecast(ts_df, arima_order, forecast_steps):
    if ts_df.empty or len(ts_df) < sum(arima_order):
        st.warning("Série temporal muito curta ou vazia para o modelo ARIMA com os parâmetros fornecidos.")
        return None, None
    
    try:
        series = ts_df['valor']
        model = ARIMA(series, order=arima_order, freq='MS')
        model_fit = model.fit()
        forecast = model_fit.get_forecast(steps=forecast_steps)
        forecast_df = forecast.summary_frame()
        return model_fit, forecast_df
    except Exception as e:
        st.error(f"Erro ao treinar o modelo ARIMA: {e}")
        st.info("Tente ajustar os parâmetros (p, d, q) do ARIMA. A série temporal pode ser não-estacionária ou ter outras características que dificultam o ajuste do modelo.")
        return None, None

def plot_forecast(series, forecast_df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=series.index, y=series, mode='lines+markers', name='Dados Históricos (Vendas Mensais)'))
    fig.add_trace(go.Scatter(x=forecast_df.index, y=forecast_df['mean'], mode='lines+markers', name='Previsão', line=dict(dash='dash', color='red')))
    fig.add_trace(go.Scatter(x=forecast_df.index, y=forecast_df['mean_ci_upper'], mode='lines', line=dict(width=0), showlegend=False))
    fig.add_trace(go.Scatter(
        x=forecast_df.index, y=forecast_df['mean_ci_lower'],
        mode='lines', line=dict(width=0),
        fill='tonexty', fillcolor='rgba(255, 0, 0, 0.2)',
        showlegend=False
    ))
    fig.update_layout(title="Previsão de Quantidade Vendida", xaxis_title="Data", yaxis_title="Quantidade Vendida", legend_title="Legenda")
    st.plotly_chart(fig, use_container_width=True)

# --- Início da Página de Previsão ---
st.title("📈 Previsão de Séries Temporais")

if 'df_principal' not in st.session_state or st.session_state.df_principal.empty:
    st.error("Os dados principais não foram carregados. Retorne à página inicial ou recarregue o aplicativo.")
    st.stop()

# --- Configurações da Previsão na Sidebar ---
st.sidebar.header("Configurações de Previsão")
st.sidebar.markdown("---") 

# A função para carregar opções agora é importada de utils
opcoes_pa, opcoes_mun = carregar_opcoes_previsao()
opcoes_pa = ["Total Geral"] + opcoes_pa
opcoes_mun = ["Total Geral"] + opcoes_mun

st.sidebar.markdown("#### 1. Selecione a Série Temporal")
filtro_pa_selecionado = st.sidebar.selectbox("Filtrar por Princípio Ativo:", options=opcoes_pa, key="forecast_pa_select")
filtro_mun_selecionado = st.sidebar.selectbox("Filtrar por Município:", options=opcoes_mun, key="forecast_mun_select")
st.sidebar.caption("Selecione 'Total Geral' para ambos para prever a quantidade total vendida.")

st.sidebar.markdown("---")
st.sidebar.markdown("#### 2. Configure o Modelo ARIMA")
st.sidebar.caption("ARIMA(p,d,q): p=ordem auto-regressiva, d=ordem de diferenciação, q=ordem de média móvel.")

col1, col2, col3 = st.sidebar.columns(3)
p = col1.number_input("p", min_value=0, max_value=10, value=5, step=1, key="arima_p")
d = col2.number_input("d", min_value=0, max_value=5, value=1, step=1, key="arima_d")
q = col3.number_input("q", min_value=0, max_value=10, value=0, step=1, key="arima_q")

horizonte_previsao = st.sidebar.slider("Meses para Prever:", min_value=1, max_value=12, value=6, key="forecast_horizon")
st.sidebar.markdown("---")

if st.sidebar.button("Executar Previsão", type="primary", use_container_width=True):
    serie_temporal = fetch_timeseries_data(filtro_pa_selecionado, filtro_mun_selecionado)
    
    if serie_temporal.empty:
        st.warning("Não foram encontrados dados para a série temporal com os filtros selecionados. Não é possível gerar a previsão.")
    else:
        st.subheader(f"Previsão para: {filtro_pa_selecionado} em {filtro_mun_selecionado}")
        modelo_ajustado, previsao_df = run_arima_forecast(serie_temporal, (p, d, q), horizonte_previsao)
        if modelo_ajustado and previsao_df is not None:
            plot_forecast(serie_temporal['valor'], previsao_df)
            with st.expander("Ver detalhes do modelo e previsão"):
                st.text("Resumo do Modelo ARIMA:")
                st.text(modelo_ajustado.summary())
                st.text("\nValores da Previsão:")
                st.dataframe(previsao_df)
else:
    st.info("Ajuste os filtros e parâmetros na barra lateral e clique em 'Executar Previsão'.")