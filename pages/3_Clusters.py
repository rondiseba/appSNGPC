# Pagina para clusterização de prescrições médicas
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from src.aplicacao.clusterizacao import agrupar_prescricoes

# --- Novas Importações dos Módulos de Utilitários ---
from src.utils.database_utils import get_duckdb_connection, TABLE_NAME

# --- Funções Auxiliares para a Página de Clusters ---
def mostrar_resultados_cluster_page(df_clusterizado, features_selecionadas, metodo_usado):
    st.subheader(f"Resultados da Clusterização com {metodo_usado}")

    if 'cluster' not in df_clusterizado.columns or df_clusterizado['cluster'].isna().all():
        st.warning("Nenhum cluster válido foi gerado. Verifique os dados ou parâmetros.")
        return

    df_resultados_display = df_clusterizado.copy()
    
    if 'sigla_uf' in df_resultados_display.columns:
        df_resultados_display['sigla_uf'] = df_resultados_display['sigla_uf'].astype(str)
    
    df_resultados_display['cluster_str'] = df_resultados_display['cluster'].astype(str)

    contagem_clusters = df_resultados_display['cluster'].value_counts().sort_index()
    st.write("Observações por Cluster (baseado na amostra):")
    st.dataframe(contagem_clusters)

    if metodo_usado.lower() == "dbscan":
        ruido_label_str = "-1"
        if ruido_label_str in df_resultados_display['cluster_str'].unique():
            try:
                original_ruido_label = df_resultados_display[df_resultados_display['cluster_str'] == ruido_label_str]['cluster'].iloc[0]
                noise_count = contagem_clusters.get(original_ruido_label, 0)
                st.info(f"No DBSCAN, o cluster {original_ruido_label} representa ruído. Total: {noise_count}")
            except IndexError:
                 st.info(f"No DBSCAN, o cluster -1 (ruído) pode estar presente.")

    numeric_features_plot = [f for f in features_selecionadas if f in df_resultados_display.columns and pd.api.types.is_numeric_dtype(df_resultados_display[f])]

    if len(numeric_features_plot) >= 2:
        x_ax, y_ax = numeric_features_plot[0], numeric_features_plot[1]
        df_plot = df_resultados_display.dropna(subset=[x_ax, y_ax, 'cluster_str']).copy()
        
        if not df_plot.empty:
            fig_scatter = px.scatter(
                df_plot, x=x_ax, y=y_ax, color='cluster_str',
                title=f"Visualização dos Clusters ({x_ax} vs {y_ax}) - Amostra",
                hover_data=[col for col in df_plot.columns if col != 'cluster_str'],
                color_discrete_map={"-1": "lightgrey"} 
            )
            fig_scatter.update_layout(legend_title_text='Cluster')
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("Não há dados suficientes para plotar o gráfico de dispersão após remover NaNs da amostra.")
    elif len(numeric_features_plot) == 1:
        x_ax = numeric_features_plot[0]
        df_plot = df_resultados_display.dropna(subset=[x_ax, 'cluster_str']).copy()
        if not df_plot.empty:
            fig_hist = px.histogram(
                df_plot, x=x_ax, color='cluster_str',
                title=f"Distribuição de {x_ax} por Cluster (Amostra)",
                barmode='overlay', marginal="box", color_discrete_map={"-1": "lightgrey"}
            )
            fig_hist.update_layout(legend_title_text='Cluster')
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.warning("Não há dados suficientes para plotar o histograma após remover NaNs da amostra.")
    else:
        st.info("Selecione pelo menos uma feature numérica para visualização gráfica dos clusters.")

    st.subheader("Estatísticas Descritivas por Cluster (Baseado na Amostra)")
    features_para_describe = [f for f in features_selecionadas if f in df_resultados_display.columns and pd.api.types.is_numeric_dtype(df_resultados_display[f])]
    if features_para_describe and 'cluster' in df_resultados_display.columns and not df_resultados_display['cluster'].isna().all():
        try:
            stats_descritivas = df_resultados_display.groupby('cluster')[features_para_describe].agg(['mean', 'median', 'std', 'count'])
            st.dataframe(stats_descritivas.style.format("{:.2f}", na_rep="-"))
        except Exception as e:
            st.warning(f"Não foi possível calcular estatísticas descritivas: {e}")
    else:
        st.info("Nenhuma feature numérica selecionada ou clusters válidos para exibir estatísticas da amostra.")

    with st.expander("Visualizar dados da amostra com labels de cluster (até 200 linhas)"):
        df_para_amostra_display = df_resultados_display.dropna(subset=['cluster'])
        st.dataframe(df_para_amostra_display.head(200), height=400)

# --- Início da Página de Clusters ---
st.title("🔍 Análise de Clusters de Prescrições")

if 'df_principal' not in st.session_state or st.session_state.df_principal.empty:
    st.error("Os dados principais não foram carregados. Retorne à página inicial ou recarregue o aplicativo.")
    st.stop()

df_referencia_features = st.session_state.df_principal
if df_referencia_features.empty:
    st.warning("Não há dados de referência disponíveis para selecionar features.")
    st.stop()

# --- Configurações da Clusterização na Sidebar ---
st.sidebar.header("Configurações de Clusterização")
st.sidebar.markdown("---")
st.sidebar.markdown("#### 1. Seleção de Features")
features_numericas_disponiveis = df_referencia_features.select_dtypes(include=np.number).columns.tolist()
default_features_sugeridas = [f for f in ['quantidade_vendida', 'idade'] if f in features_numericas_disponiveis]
if not default_features_sugeridas and features_numericas_disponiveis:
    default_features_sugeridas = features_numericas_disponiveis[:min(2, len(features_numericas_disponiveis))]

features_selecionadas_cluster_page = st.sidebar.multiselect(
    "Selecione as features numéricas:",
    options=features_numericas_disponiveis,
    default=default_features_sugeridas,
    key="cluster_page_features_select"
)
st.sidebar.caption("Escolha colunas numéricas relevantes para a formação dos grupos.")

st.sidebar.markdown("---") 
st.sidebar.markdown("#### 2. Amostragem de Dados")
sample_size = st.sidebar.number_input(
    "Tamanho da Amostra:",
    min_value=100, max_value=50000, value=10000, step=100,
    key="cluster_page_sample_size",
    help="Número de registros a serem amostrados aleatoriamente para a análise."
)
st.sidebar.caption("Amostras maiores podem ser mais representativas, mas aumentam o tempo de processamento.")

st.sidebar.markdown("---") 
st.sidebar.markdown("#### 3. Método e Parâmetros")
metodo_cluster_selecionado_page = st.sidebar.selectbox(
    "Escolha o Método:",
    options=["KMeans", "DBSCAN"], index=0, key="cluster_page_method_select"
)

params_cluster_page = {}
if metodo_cluster_selecionado_page == "KMeans":
    st.sidebar.caption("K-Means particiona os dados em 'K' clusters esféricos.")
    params_cluster_page['kmeans_n_clusters'] = st.sidebar.slider("Número de Clusters (K):", min_value=2, max_value=15, value=4, step=1, key="cluster_page_kmeans_k")
    st.sidebar.caption("O 'K' ideal pode ser estimado com métodos como Elbow ou Silhouette Analysis.")
elif metodo_cluster_selecionado_page == "DBSCAN":
    st.sidebar.caption("DBSCAN agrupa pontos em áreas de alta densidade, marcando outliers como ruído.")
    params_cluster_page['dbscan_eps'] = st.sidebar.slider("Epsilon (eps):", min_value=0.05, max_value=5.0, value=0.5, step=0.05, key="cluster_page_dbscan_eps")
    st.sidebar.caption("'eps': Raio da vizinhança para um ponto ser considerado vizinho.")
    params_cluster_page['dbscan_min_samples'] = st.sidebar.slider("Mín. Amostras:", min_value=2, max_value=100, value=5, step=1, key="cluster_page_dbscan_min")
    st.sidebar.caption("'min_samples': Nº mínimo de pontos para formar uma região densa.")

st.sidebar.markdown("---") 

if not features_selecionadas_cluster_page or len(features_selecionadas_cluster_page) < 1:
    st.info("Por favor, selecione pelo menos uma feature numérica na barra lateral para a clusterização.")
else:
    if st.sidebar.button("Executar Clusterização", type="primary", key="cluster_page_run_button", use_container_width=True):
        conn = get_duckdb_connection()
        if conn is None:
            st.error("Não foi possível conectar ao banco de dados para buscar dados para clusterização.")
            st.stop()

        select_features_sql = ", ".join([f'"{f}"' for f in features_selecionadas_cluster_page])
        where_clause_cluster_data = "WHERE ano IN (2019, 2020)"
            
        query_cluster_data = f"""
            SELECT {select_features_sql} 
            FROM {TABLE_NAME}
            {where_clause_cluster_data}
            USING SAMPLE {sample_size} ROWS;
        """
        
        st.info(f"Buscando {sample_size} amostras com as features: {', '.join(features_selecionadas_cluster_page)} para clusterização...")
        try:
            df_para_clusterizar = conn.execute(query_cluster_data).fetchdf()
        except Exception as e_query:
            st.error(f"Erro ao buscar dados do DuckDB para clusterização: {e_query}")
            st.stop()

        if df_para_clusterizar.empty:
            st.warning(f"A amostra de dados ({sample_size} registros) buscada do banco está vazia. Verifique se há dados suficientes para as features selecionadas.")
            st.stop()
        
        with st.spinner(f"Aplicando o algoritmo {metodo_cluster_selecionado_page} em {len(df_para_clusterizar)} amostras..."):
            df_clusterizado_resultado = agrupar_prescricoes(
                df_para_clusterizar, 
                metodo=metodo_cluster_selecionado_page.lower(),
                features=features_selecionadas_cluster_page,
                **params_cluster_page
            )
        
        if df_clusterizado_resultado is None or 'cluster' not in df_clusterizado_resultado.columns or df_clusterizado_resultado['cluster'].isna().all():
            st.error("A clusterização falhou ou não produziu clusters válidos. Verifique os parâmetros, os dados de amostra e a função 'agrupar_prescricoes'.")
        else:
            st.success(f"Clusterização com {metodo_cluster_selecionado_page} concluída na amostra de {len(df_clusterizado_resultado)} registros!")
            st.session_state['df_clusterizado_cache_page'] = df_clusterizado_resultado
            st.session_state['features_usadas_cache_page'] = features_selecionadas_cluster_page
            st.session_state['metodo_usado_cache_page'] = metodo_cluster_selecionado_page
            
            mostrar_resultados_cluster_page(
                df_clusterizado_resultado, 
                features_selecionadas_cluster_page, 
                metodo_cluster_selecionado_page
            )

    elif 'df_clusterizado_cache_page' in st.session_state:
        st.info("Exibindo resultados da última clusterização (baseada em amostra). Modifique os parâmetros e clique em 'Executar' para atualizar.")
        df_cache = st.session_state['df_clusterizado_cache_page'].copy()
        if 'sigla_uf' in df_cache.columns:
            df_cache['sigla_uf'] = df_cache['sigla_uf'].astype(str)
        
        mostrar_resultados_cluster_page(
            df_cache, 
            st.session_state['features_usadas_cache_page'],
            st.session_state['metodo_usado_cache_page']
        )
    else:
        st.info("Ajuste os parâmetros de clusterização na barra lateral e clique em 'Executar Clusterização' para ver os resultados.")

st.markdown("---")
st.caption("A análise de clusters (realizada em uma amostra dos dados) pode ajudar a identificar grupos de prescrições com características similares.")