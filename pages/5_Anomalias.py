# Pagina para Detecção de Anomalias com Isolation Forest
# Baseado no modelo de previsão de séries temporais
import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from sklearn.ensemble import IsolationForest
from src.utils.database_utils import get_duckdb_connection, carregar_opcoes_previsao, TABLE_NAME

# --- Funções Específicas da Página (Busca de dados e Modelagem) ---

@st.cache_data(show_spinner="Buscando amostra de dados para análise de anomalias...")
def fetch_data_for_anomaly(features, filtro_pa, filtro_mun, sample_size, tabela=TABLE_NAME):
    """
    Busca uma amostra de dados do DuckDB com base nos filtros e features selecionados.
    """
    conn = get_duckdb_connection()
    if conn is None: return pd.DataFrame()

    select_clause = ", ".join([f'"{f}"' for f in features])
    
    conditions = ["ano IN (2019, 2020)"]
    params = []
    
    if filtro_pa != "Todos":
        conditions.append("principio_ativo = ?")
        params.append(filtro_pa)
    if filtro_mun != "Todos":
        conditions.append("nome_municipio = ?")
        params.append(filtro_mun)
    
    not_null_conditions = " AND ".join([f'"{f}" IS NOT NULL' for f in features])
    if not_null_conditions:
        conditions.append(not_null_conditions)

    where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT {select_clause} 
        FROM {tabela}
        {where_clause}
        USING SAMPLE {sample_size} ROWS;
    """
    try:
        df_sample = conn.execute(query, params).fetchdf()
        return df_sample
    except Exception as e:
        st.error(f"Erro ao buscar dados do DuckDB: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner="Detectando anomalias com Isolation Forest...")
def run_isolation_forest(df, features, contamination):
    """
    Executa o algoritmo Isolation Forest em um DataFrame.
    """
    if df.empty:
        st.warning("DataFrame de entrada para o modelo está vazio.")
        return pd.DataFrame()
    
    df_clean = df[features].dropna()
    
    if len(df_clean) < 2:
        st.warning(f"Após remover valores ausentes, restam poucos dados ({len(df_clean)}) para a detecção de anomalias.")
        return pd.DataFrame()
        
    try:
        model = IsolationForest(contamination=contamination, random_state=42)
        model.fit(df_clean)
        
        # Adiciona a predição de anomalia (-1 para anomalias, 1 para normais)
        df_clean['anomaly'] = model.predict(df_clean)
        
        return df_clean
    except Exception as e:
        st.error(f"Erro ao executar o Isolation Forest: {e}")
        return pd.DataFrame()

def plot_anomalies(df_result, features):
    """
    Plota as anomalias detectadas.
    """
    df_result['Status'] = df_result['anomaly'].apply(lambda x: 'Anomalia' if x == -1 else 'Normal')
    
    if len(features) >= 2:
        st.subheader("Visualização das Anomalias (2D)")
        x_ax, y_ax = features[0], features[1]
        fig = px.scatter(
            df_result, x=x_ax, y=y_ax, color='Status',
            color_discrete_map={'Normal': '#1f77b4', 'Anomalia': '#d62728'}, # Azul e Vermelho
            title=f"Detecção de Anomalias com Isolation Forest ({x_ax} vs {y_ax})"
        )
        st.plotly_chart(fig, use_container_width=True)
    elif len(features) == 1:
        st.subheader(f"Distribuição de '{features[0]}' para Pontos Normais vs. Anômalos")
        fig = px.histogram(
            df_result, x=features[0], color="Status",
            marginal="box", color_discrete_map={'Normal': '#1f77b4', 'Anomalia': '#d62728'},
            barmode="overlay", opacity=0.7
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Selecione 1 ou 2 features para visualizar as anomalias graficamente.")

# --- Início da Página de Anomalias ---
st.title("🚨 Detecção de Anomalias")
st.write("Use esta página para identificar prescrições com padrões incomuns usando o algoritmo Isolation Forest.")

if 'df_principal' not in st.session_state or st.session_state.df_principal.empty:
    st.error("Os dados principais não foram carregados. Retorne à página inicial ou recarregue o aplicativo.")
    st.stop()

df_referencia_anomalias = st.session_state.df_principal

# --- Configurações na Sidebar ---
st.sidebar.header("Configurações de Anomalias")
st.sidebar.markdown("---")

st.sidebar.markdown("#### 1. Filtrar Dados (Opcional)")
# Reutilizando a função de carregar opções que já está em utils
opcoes_pa_anomalia, opcoes_mun_anomalia = carregar_opcoes_previsao()
filtro_pa_anomalia = st.sidebar.selectbox("Analisar um Princípio Ativo específico:", options=["Todos"] + opcoes_pa_anomalia, key="anomaly_pa_select")
filtro_mun_anomalia = st.sidebar.selectbox("Analisar um Município específico:", options=["Todos"] + opcoes_mun_anomalia, key="anomaly_mun_select")

st.sidebar.markdown("---")
st.sidebar.markdown("#### 2. Configurar Modelo")
features_numericas_disponiveis = df_referencia_anomalias.select_dtypes(include=np.number).columns.tolist()
default_features_anomalia = [f for f in ['quantidade_vendida', 'idade'] if f in features_numericas_disponiveis]
features_selecionadas_anomalia = st.sidebar.multiselect(
    "Selecione as features para análise:",
    options=features_numericas_disponiveis,
    default=default_features_anomalia,
    key="anomaly_features_select",
    help="O modelo buscará anomalias com base na combinação destas características."
)

sample_size_anomalia = st.sidebar.number_input(
    "Tamanho da Amostra:", min_value=1000, max_value=100000, value=20000, step=1000, key="anomaly_sample_size",
    help="Número de registros a serem amostrados aleatoriamente para a análise."
)

contamination_anomalia = st.sidebar.slider(
    "Contaminação (proporção de anomalias):",
    min_value=0.001, max_value=0.1, value=0.01, step=0.001,
    format="%.3f", key="anomaly_contamination_slider",
    help="A proporção de anomalias esperada no conjunto de dados. Parâmetro importante do Isolation Forest."
)
st.sidebar.markdown("---")

if not features_selecionadas_anomalia:
    st.info("Por favor, selecione pelo menos uma feature na barra lateral para a análise.")
else:
    if st.sidebar.button("Detectar Anomalias", type="primary", use_container_width=True):
        
        df_amostra_anomalia = fetch_data_for_anomaly(
            features=features_selecionadas_anomalia,
            filtro_pa=filtro_pa_anomalia,
            filtro_mun=filtro_mun_anomalia,
            sample_size=sample_size_anomalia
        )
        
        if df_amostra_anomalia.empty:
            st.warning("Nenhuma amostra de dados foi retornada com os filtros e features selecionados. Tente ampliar a busca.")
        else:
            df_resultado_anomalia = run_isolation_forest(
                df=df_amostra_anomalia,
                features=features_selecionadas_anomalia,
                contamination=contamination_anomalia
            )
            
            if not df_resultado_anomalia.empty:
                st.success("Análise de anomalias concluída!")
                
                df_anomalias_encontradas = df_resultado_anomalia[df_resultado_anomalia['anomaly'] == -1]
                num_anomalias = len(df_anomalias_encontradas)
                total_analisado = len(df_resultado_anomalia)
                
                st.metric(
                    label="Anomalias Detectadas na Amostra",
                    value=f"{num_anomalias}",
                    delta=f"{num_anomalias / total_analisado:.2%} do total analisado" if total_analisado > 0 else "0.00%",
                    delta_color="inverse"
                )
                
                plot_anomalies(df_resultado_anomalia, features_selecionadas_anomalia)
                
                with st.expander(f"Ver os {num_anomalias} registros anômalos detalhadamente"):
                    st.dataframe(df_anomalias_encontradas)
            else:
                st.error("Não foi possível executar a análise de anomalias. Verifique os avisos acima.")

    else:
        st.info("Ajuste os filtros e parâmetros e clique em 'Detectar Anomalias' para iniciar a análise.")