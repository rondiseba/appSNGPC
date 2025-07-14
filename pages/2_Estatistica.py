# Pagina para analise estatistca e graficos descritivos
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from scipy.stats import ttest_ind, mannwhitneyu, levene

# --- Importações dos Módulos de Utilitários ---
from src.utils.database_utils import get_duckdb_connection, TABLE_NAME
from src.utils.stats_utils import realizar_teste_shapiro, realizar_teste_anova

# --- Início da Página de Análise Estatística ---
st.title("📊 Análise Estatística Avançada")

if 'df_principal' not in st.session_state or st.session_state.df_principal.empty:
    st.error("Os dados principais (2019-2020) não foram carregados. Retorne à página inicial.")
    st.stop()

conn_stats_page = get_duckdb_connection()
if conn_stats_page is None:
    st.error("Não foi possível conectar ao banco de dados DuckDB.")
    st.stop()

# --- Estrutura de Abas ---
tab_comparativo_anual_stats, tab_geral = st.tabs([
    "🆚 Comparativo Estatístico Detalhado (2019 vs. 2020)",
    "🔎 Análise Geral (2019-2020 Combinados)"
])

# --- Aba: Comparativo Estatístico Detalhado ---
with tab_comparativo_anual_stats:
    st.header("Comparativo Detalhado: 2019 vs. 2020")
    st.subheader("Estatísticas Descritivas Comparativas")
    
    colunas_numericas_desc = ['quantidade_vendida', 'idade']
    desc_dfs_list_comparativo = []

    for col_desc_comp in colunas_numericas_desc:
        # CORREÇÃO: Definindo as queries de 2019 e 2020 explicitamente para evitar erros.
        query_desc_2019_comp = f"""
            SELECT 
                '{col_desc_comp}_2019' as "Statistic_Name", 
                COUNT("{col_desc_comp}") as "count", AVG("{col_desc_comp}") as "mean",
                STDDEV_SAMP("{col_desc_comp}") as "std", MIN("{col_desc_comp}") as "min",
                MEDIAN("{col_desc_comp}") as "50%",
                MAX("{col_desc_comp}") as "max"
            FROM {TABLE_NAME} WHERE ano = 2019 AND "{col_desc_comp}" IS NOT NULL;
        """
        query_desc_2020_comp = f"""
            SELECT 
                '{col_desc_comp}_2020' as "Statistic_Name",
                COUNT("{col_desc_comp}") as "count", AVG("{col_desc_comp}") as "mean",
                STDDEV_SAMP("{col_desc_comp}") as "std", MIN("{col_desc_comp}") as "min",
                MEDIAN("{col_desc_comp}") as "50%",
                MAX("{col_desc_comp}") as "max"
            FROM {TABLE_NAME} WHERE ano = 2020 AND "{col_desc_comp}" IS NOT NULL;
        """
        
        try:
            df_desc_2019_intermediate = conn_stats_page.execute(query_desc_2019_comp).fetchdf()
            if not df_desc_2019_intermediate.empty:
                df_desc_2019_col = df_desc_2019_intermediate.set_index("Statistic_Name").T 
                desc_dfs_list_comparativo.append(df_desc_2019_col)
                
            df_desc_2020_intermediate = conn_stats_page.execute(query_desc_2020_comp).fetchdf()
            if not df_desc_2020_intermediate.empty:
                df_desc_2020_col = df_desc_2020_intermediate.set_index("Statistic_Name").T
                desc_dfs_list_comparativo.append(df_desc_2020_col)
        except Exception as e:
            st.warning(f"Não foi possível calcular estatísticas descritivas para '{col_desc_comp}': {e}")

    if desc_dfs_list_comparativo:
        df_desc_final_comparativo = pd.concat(desc_dfs_list_comparativo, axis=1) 
        st.dataframe(df_desc_final_comparativo.style.format("{:.2f}", na_rep="-"))
    else:
        st.info("Nenhuma estatística descritiva pôde ser calculada para o comparativo.")

    max_sample_boxplot = 20000
    for col_plot_desc_comp in colunas_numericas_desc:
        query_boxplot = f"""
            SELECT ano, "{col_plot_desc_comp}" 
            FROM {TABLE_NAME} 
            WHERE ano IN (2019, 2020) AND "{col_plot_desc_comp}" IS NOT NULL
            USING SAMPLE {max_sample_boxplot} ROWS; 
        """
        try:
            df_plot_box_data_comp = conn_stats_page.execute(query_boxplot).fetchdf()
            if not df_plot_box_data_comp.empty:
                st.markdown(f"##### Distribuição Comparativa de '{col_plot_desc_comp}' (Amostra)")
                df_plot_box_data_comp['ano'] = df_plot_box_data_comp['ano'].astype(str)
                fig_box_comp = px.box(df_plot_box_data_comp, x='ano', y=col_plot_desc_comp, 
                                color='ano', points=False, 
                                labels={'ano':'Ano', col_plot_desc_comp:col_plot_desc_comp.replace("_"," ").capitalize()},
                                title=f"Distribuição de {col_plot_desc_comp.replace('_',' ').capitalize()} por Ano (Amostra)")
                st.plotly_chart(fig_box_comp, use_container_width=True)
            else:
                st.info(f"Sem dados (ou amostra vazia) para boxplot de '{col_plot_desc_comp}'.")
        except Exception as e:
            st.warning(f"Não foi possível gerar boxplot para '{col_plot_desc_comp}': {e}")
    st.markdown("---")

    st.subheader("Comparativo de 'Quantidade Vendida' entre 2019 e 2020")
    try:
        qtd_2019_series = conn_stats_page.execute(f"SELECT quantidade_vendida FROM {TABLE_NAME} WHERE ano = 2019 AND quantidade_vendida IS NOT NULL;").fetchdf()['quantidade_vendida']
        qtd_2020_series = conn_stats_page.execute(f"SELECT quantidade_vendida FROM {TABLE_NAME} WHERE ano = 2020 AND quantidade_vendida IS NOT NULL;").fetchdf()['quantidade_vendida']
        if qtd_2019_series.empty or qtd_2020_series.empty or len(qtd_2019_series) < 3 or len(qtd_2020_series) < 3:
            st.warning("Dados insuficientes de 'quantidade_vendida' em 2019 ou 2020 para testes estatísticos.")
        else:
            st.markdown("###### Teste de Normalidade (Shapiro-Wilk)")
            stat_s19, p_s19, err_s19, msg_s19 = realizar_teste_shapiro(qtd_2019_series, 'Qtd Vendida 2019')
            stat_s20, p_s20, err_s20, msg_s20 = realizar_teste_shapiro(qtd_2020_series, 'Qtd Vendida 2020')
            alpha = 0.05; norm_2019 = False; norm_2020 = False
            if err_s19: st.warning(f"2019 (Shapiro): {err_s19}")
            elif stat_s19 is not None : 
                norm_2019 = p_s19 > alpha
                st.write(f"2019: W={stat_s19:.4f}, p-valor={p_s19:.4f} ({'Normal' if norm_2019 else 'Não-Normal'})")
                if msg_s19: st.caption(msg_s19)
            if err_s20: st.warning(f"2020 (Shapiro): {err_s20}")
            elif stat_s20 is not None: 
                norm_2020 = p_s20 > alpha
                st.write(f"2020: W={stat_s20:.4f}, p-valor={p_s20:.4f} ({'Normal' if norm_2020 else 'Não-Normal'})")
                if msg_s20: st.caption(msg_s20)
            homogeneidade_variancias = False
            if (stat_s19 is not None and not err_s19) and (stat_s20 is not None and not err_s20) and norm_2019 and norm_2020 :
                st.markdown("###### Teste de Homogeneidade de Variâncias (Levene)")
                try:
                    n_subsample_levene = 10000
                    s1_levene = qtd_2019_series.sample(n=min(len(qtd_2019_series), n_subsample_levene), random_state=42) if len(qtd_2019_series) > n_subsample_levene else qtd_2019_series
                    s2_levene = qtd_2020_series.sample(n=min(len(qtd_2020_series), n_subsample_levene), random_state=42) if len(qtd_2020_series) > n_subsample_levene else qtd_2020_series
                    if len(s1_levene) < 2 or len(s2_levene) < 2: st.info("Subamostras para Levene muito pequenas, teste não realizado.")
                    else:
                        stat_l, p_l = levene(s1_levene, s2_levene)
                        homogeneidade_variancias = p_l > alpha
                        st.write(f"Teste de Levene (subamostras até {n_subsample_levene:,}): Estatística={stat_l:.4f}, p-valor={p_l:.4f} ({'Variâncias homogêneas' if homogeneidade_variancias else 'Variâncias não homogêneas'})")
                except Exception as e_levene: st.warning(f"Erro Teste Levene: {e_levene}")
            st.markdown("###### Teste de Comparação (2019 vs 2020)")
            if (stat_s19 is not None and not err_s19) and (stat_s20 is not None and not err_s20):
                if norm_2019 and norm_2020:
                    try:
                        stat_t, p_t = ttest_ind(qtd_2019_series, qtd_2020_series, equal_var=homogeneidade_variancias, nan_policy='omit')
                        st.write(f"Teste t (equal_var={homogeneidade_variancias}): Estatística={stat_t:.4f}, p-valor={p_t:.4f}")
                        st.write(f"Interpretação: **{'Médias estatisticamente diferentes' if p_t < alpha else 'Não há diferença estatística significativa entre as médias'}**.")
                    except Exception as e_ttest: st.warning(f"Erro Teste t: {e_ttest}")
                else:
                    st.write("Aplicando teste não-paramétrico Mann-Whitney U...")
                    try:
                        n_subsample_mw = 10000; msg_mw_subsampling = ""
                        s1_mw = qtd_2019_series
                        if len(s1_mw) > n_subsample_mw: s1_mw = s1_mw.sample(n=n_subsample_mw, random_state=42); msg_mw_subsampling += f"Amostra 2019 ({len(qtd_2019_series):,}) subamostrada para {n_subsample_mw:,}. "
                        s2_mw = qtd_2020_series
                        if len(s2_mw) > n_subsample_mw: s2_mw = s2_mw.sample(n=n_subsample_mw, random_state=42); msg_mw_subsampling += f"Amostra 2020 ({len(qtd_2020_series):,}) subamostrada para {n_subsample_mw:,}."
                        if msg_mw_subsampling: st.caption(msg_mw_subsampling)
                        if len(s1_mw) < 1 or len(s2_mw) < 1: st.warning("Dados insuficientes para Mann-Whitney U.")
                        else:
                            stat_mw, p_mw = mannwhitneyu(s1_mw, s2_mw, alternative='two-sided')
                            st.write(f"Mann-Whitney U (subamostras): U={stat_mw:.0f}, p-valor={p_mw:.4f}")
                            st.write(f"Interpretação: **{'Distribuições estatisticamente diferentes' if p_mw < alpha else 'Não há diferença estatística significativa'}**.")
                    except Exception as e_mw: st.warning(f"Erro Mann-Whitney U: {e_mw}")
            else: st.info("Testes de comparação não realizados devido a erro nos testes de normalidade.")
    except Exception as e_fetch_qtd:
        st.error(f"Erro ao buscar dados de quantidade vendida do DB: {e_fetch_qtd}")
    st.markdown("---")

# --- Aba: Análise Geral (2019-2020 Combinados) ---
with tab_geral:
    st.header("Análise do Conjunto de Dados Combinado (2019-2020)")
    col_testes, col_correlacao = st.columns(2)
    with col_testes:
        st.subheader("Testes de Hipótese (Dados Combinados)")
        st.markdown("Análise da 'Quantidade Vendida'.")
        st.markdown("#### Teste de Normalidade (Shapiro-Wilk)")
        try:
            series_qtd_total = conn_stats_page.execute(f"SELECT quantidade_vendida FROM {TABLE_NAME} WHERE ano IN (2019,2020) AND quantidade_vendida IS NOT NULL;").fetchdf()['quantidade_vendida']
            if series_qtd_total.empty:
                st.warning("Sem dados de 'quantidade_vendida' para teste de normalidade.")
            else:
                stat_s, p_s, err_s, msg_s_geral = realizar_teste_shapiro(series_qtd_total, 'Qtd Vendida (Total)')
                if err_s: st.warning(err_s)
                elif stat_s is not None:
                    st.write(f"Para 'Quantidade Vendida' (dados combinados): W={stat_s:.4f}, p-valor={p_s:.4f} ({'Normal' if p_s > 0.05 else 'Não-Normal'})")
                    if msg_s_geral: st.caption(msg_s_geral)
        except Exception as e_shapiro_total:
            st.error(f"Erro ao buscar dados para Shapiro (total): {e_shapiro_total}")

        st.markdown("---")
        st.markdown("#### Teste ANOVA (para 'Quantidade Vendida' entre grupos)")
        opcoes_grupo_anova_orig = [col for col in ['ano', 'sexo', 'faixa_etaria', 'nome_municipio', 'principio_ativo'] if col in st.session_state.df_principal.columns]
        opcoes_grupo_anova_validas = []
        for col_anova in opcoes_grupo_anova_orig:
            try:
                unique_count_df = conn_stats_page.execute(f'SELECT COUNT(DISTINCT "{col_anova}") AS count FROM {TABLE_NAME} WHERE ano IN (2019,2020) AND "{col_anova}" IS NOT NULL;').fetchdf()
                if not unique_count_df.empty:
                    unique_count = unique_count_df['count'].iloc[0]
                    if 2 <= unique_count < 50:
                        opcoes_grupo_anova_validas.append(col_anova)
            except Exception: pass 
        if not opcoes_grupo_anova_validas:
            st.warning("Nenhuma coluna de agrupamento adequada encontrada para ANOVA.")
        else:
            default_idx_anova = opcoes_grupo_anova_validas.index('ano') if 'ano' in opcoes_grupo_anova_validas else 0
            grupo_anova_selecionado = st.selectbox("Variável de agrupamento para ANOVA:", options=opcoes_grupo_anova_validas, index=default_idx_anova, key="anova_grupo_select_geral")
            if grupo_anova_selecionado:
                try:
                    df_anova_data = conn_stats_page.execute(f'SELECT quantidade_vendida, "{grupo_anova_selecionado}" FROM {TABLE_NAME} WHERE ano IN (2019,2020) AND quantidade_vendida IS NOT NULL AND "{grupo_anova_selecionado}" IS NOT NULL;').fetchdf()
                    if df_anova_data.empty or df_anova_data['quantidade_vendida'].isnull().all() or df_anova_data[grupo_anova_selecionado].isnull().all():
                        st.warning(f"Dados insuficientes para ANOVA com grupo '{grupo_anova_selecionado}'.")
                    else:
                        stat_a, p_a, err_a = realizar_teste_anova(df_anova_data, 'quantidade_vendida', grupo_anova_selecionado) 
                        if err_a: st.warning(f"Erro ANOVA: {err_a}")
                        elif stat_a is not None:
                            st.write(f"ANOVA para 'Qtd Vendida' por '{grupo_anova_selecionado}': F={stat_a:.4f}, p-valor={p_a:.4f} ({'Diferença significativa' if p_a <= 0.05 else 'Sem diferença significativa'})")
                except Exception as e_anova_data:
                    st.error(f"Erro ao buscar dados para ANOVA: {e_anova_data}")

    with col_correlacao:
        st.subheader("Análise de Correlação (Dados Combinados)")
        try:
            schema_df = conn_stats_page.execute(f"DESCRIBE {TABLE_NAME};").fetchdf()
            colunas_numericas_db = schema_df[schema_df['column_type'].str.contains('INT|FLOAT|DOUBLE|DECIMAL', case=False)]['column_name'].tolist()
            colunas_numericas_db = [c for c in colunas_numericas_db if c not in ['ano', 'mes', 'id_municipio_6dig', 'id_paciente']] 
        except Exception:
            colunas_numericas_db = ['idade', 'quantidade_vendida'] 
        if len(colunas_numericas_db) < 2: st.warning("Menos de duas colunas numéricas adequadas encontradas para correlação.")
        else:
            default_corr_cols = [col for col in ['idade', 'quantidade_vendida'] if col in colunas_numericas_db]
            if len(default_corr_cols) < 2 and len(colunas_numericas_db) >=2 : default_corr_cols = colunas_numericas_db[:2]
            colunas_para_corr = st.multiselect("Colunas para matriz de correlação:", options=colunas_numericas_db, default=default_corr_cols, key="corr_cols_multiselect_geral")
            metodo_correlacao = st.radio("Método de correlação:", options=["pearson", "spearman"], index=0, horizontal=True, key="corr_method_radio_geral")
            if len(colunas_para_corr) >= 2:
                try:
                    select_cols_corr = ", ".join([f'"{c}"' for c in colunas_para_corr])
                    n_subsample_corr_query = 50000
                    df_corr_data = conn_stats_page.execute(f"SELECT {select_cols_corr} FROM {TABLE_NAME} WHERE ano IN (2019,2020) USING SAMPLE {n_subsample_corr_query} ROWS;").fetchdf()
                    st.caption(f"Correlação calculada em uma amostra de até {n_subsample_corr_query:,} linhas.")
                    df_corr_data.dropna(inplace=True)
                    if len(df_corr_data) < 2: st.warning("Dados insuficientes para correlação após remover NaNs da amostra.")
                    else:
                        n_subsample_pandas_corr = 20000; msg_corr_subsampling = ""
                        df_corr_final = df_corr_data
                        if len(df_corr_data) > n_subsample_pandas_corr:
                            df_corr_final = df_corr_data.sample(n=n_subsample_pandas_corr, random_state=42)
                            msg_corr_subsampling = f" Matriz final em subamostra adicional de {n_subsample_pandas_corr:,} pontos."
                        matriz_correlacao = df_corr_final.corr(method=metodo_correlacao)
                        st.write(f"**Matriz de Correlação ({metodo_correlacao.capitalize()})**")
                        if msg_corr_subsampling: st.caption(msg_corr_subsampling)
                        st.dataframe(matriz_correlacao.style.background_gradient(cmap='coolwarm', axis=None, vmin=-1, vmax=1).format("{:.2f}"))
                        if not matriz_correlacao.empty:
                            fig_heatmap_corr = px.imshow(matriz_correlacao, text_auto=".2f", aspect="auto", color_continuous_scale='RdBu_r', title=f"Mapa de Calor da Correlação ({metodo_correlacao.capitalize()})")
                            st.plotly_chart(fig_heatmap_corr, use_container_width=True)
                except Exception as e_corr_data:
                    st.error(f"Erro ao buscar ou processar dados para correlação: {e_corr_data}")
            elif colunas_para_corr: st.info("Selecione pelo menos duas colunas numéricas para correlação.")

st.markdown("---")
st.caption("Nota: Testes estatísticos e correlações são ferramentas exploratórias. A interpretação deve considerar o contexto, tamanho da amostra e suposições dos testes.")