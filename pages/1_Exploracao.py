# Pagina para exploraçao de dados

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

from src.utils.database_utils import (
    get_duckdb_connection,
    build_where_clause,
    carregar_opcoes_filtro_do_db,
    TABLE_NAME
)

# --- Funções SQL para Métricas e Gráficos ---

@st.cache_data(show_spinner="Buscando amostra de dados...")
def fetch_sample_data_from_duckdb(filtros, tabela=TABLE_NAME, limit=1000):
    conn = get_duckdb_connection()
    if conn is None: return pd.DataFrame()
    where_clause, params = build_where_clause(filtros)
    query = f"SELECT * FROM {tabela} {where_clause} LIMIT {limit};"
    try:
        return conn.execute(query, params).fetchdf()
    except Exception as e:
        st.error(f"Erro ao buscar amostra de dados: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner="Calculando métricas...")
def get_visao_geral_metricas(filtros, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return 0, 0, 0
    where_clause, params = build_where_clause(filtros)
    try:
        total_registros = conn.execute(f"SELECT COUNT(*) FROM {tabela} {where_clause};", params).fetchone()[0]
        municipios_unicos = conn.execute(f"SELECT COUNT(DISTINCT nome_municipio) FROM {tabela} {where_clause} AND nome_municipio IS NOT NULL;", params).fetchone()[0]
        principios_unicos = conn.execute(f"SELECT COUNT(DISTINCT principio_ativo) FROM {tabela} {where_clause} AND principio_ativo IS NOT NULL;", params).fetchone()[0]
    except Exception as e:
        st.error(f"Erro ao calcular métricas: {e}")
        return 0, 0, 0
    return total_registros, municipios_unicos, principios_unicos

@st.cache_data(show_spinner="Gerando gráfico de Top 10...")
def plot_top_principios_sql(filtros, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return
    where_clause, params = build_where_clause(filtros)
    query = f'SELECT principio_ativo AS "Princípio Ativo", COUNT(*) AS "Total" FROM {tabela} {where_clause} AND principio_ativo IS NOT NULL GROUP BY "Princípio Ativo" ORDER BY "Total" DESC LIMIT 10;'
    try:
        top_meds_df = conn.execute(query, params).fetchdf()
        if top_meds_df.empty:
            st.info("Nenhum dado de princípios ativos encontrado com os filtros selecionados para o top 10.")
            return
        fig = px.bar(top_meds_df, x='Total', y='Princípio Ativo', orientation='h', text_auto='.2s')
        fig.update_layout(
            title_text='Top 10 Princípios Ativos Mais Prescritos', title_font_size=16, title_x=0.5,
            yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=40, b=0)
        )
        fig.update_traces(hovertemplate="%{y}<br>Total: %{x:,} prescrições", marker_color='#3498db')
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e: st.error(f"Erro ao gerar top princípios ativos: {e}")

@st.cache_data(show_spinner="Gerando gráfico de evolução temporal...")
def plot_evolucao_temporal_sql(filtros, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return
    where_clause, params = build_where_clause(filtros)
    query = f"SELECT strftime(data, '%Y-%m-01') AS mes_ano, SUM(quantidade_vendida) AS total_quantidade_vendida FROM {tabela} {where_clause} AND data IS NOT NULL AND quantidade_vendida IS NOT NULL GROUP BY mes_ano ORDER BY mes_ano ASC;"
    try:
        df_mensal = conn.execute(query, params).fetchdf()
        if df_mensal.empty:
            st.info("Não há dados agregados mensalmente para exibir a evolução temporal.")
            return
        df_mensal['mes_ano'] = pd.to_datetime(df_mensal['mes_ano'])
        fig = px.line(df_mensal, x='mes_ano', y='total_quantidade_vendida', markers=True, labels={'total_quantidade_vendida': 'Total Vendido', 'mes_ano': 'Data'})
        fig.update_layout(title_text='Evolução Mensal da Quantidade Vendida', title_font_size=18, title_x=0.5)
        fig.update_traces(line_color='#e74c3c', hovertemplate="Data: %{x|%b/%Y}<br>Quantidade: %{y:,.0f} unidades")
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e: st.error(f"Erro ao gerar evolução temporal: {e}")

@st.cache_data(show_spinner="Gerando gráfico de distribuição de idades...")
def plot_distribuicao_idades_sql(filtros, num_bins=20, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return
    where_clause, params = build_where_clause(filtros)
    query_min_max = f"SELECT MIN(idade), MAX(idade) FROM {tabela} {where_clause} AND idade IS NOT NULL;"
    try:
        min_max_result = conn.execute(query_min_max, params).fetchone()
        if min_max_result is None or min_max_result[0] is None:
            st.info("Não há dados de idade válidos.")
            return
        min_age, max_age = min_max_result
        if min_age == max_age: bin_width = 1
        else: bin_width = max(1, np.ceil((max_age - min_age) / num_bins))
        query_hist = f"SELECT CAST(FLOOR((idade - {min_age}) / {bin_width}) * {bin_width} + {min_age} AS INTEGER) AS bin_start, COUNT(*) as \"Contagem\" FROM {tabela} {where_clause} AND idade IS NOT NULL GROUP BY bin_start ORDER BY bin_start;"
        df_hist_data = conn.execute(query_hist, params).fetchdf()
        if df_hist_data.empty:
            st.info("Não há dados para exibir a distribuição de idade.")
            return
        df_hist_data["Faixa de Idade"] = df_hist_data["bin_start"].astype(str) + " - " + (df_hist_data["bin_start"] + bin_width - 1).astype(str)
        fig = px.bar(df_hist_data, x='Faixa de Idade', y='Contagem', labels={'Contagem': 'Nº de Prescrições'})
        fig.update_layout(title_text='Distribuição de Idades', title_font_size=16, title_x=0.5)
        st.plotly_chart(fig, use_container_width=True)
        query_stats_age = f"SELECT AVG(idade), MEDIAN(idade) FROM {tabela} {where_clause} AND idade IS NOT NULL;"
        avg_age, median_age = conn.execute(query_stats_age, params).fetchone()
        if avg_age is not None and median_age is not None:
            st.caption(f"Mediana: {median_age:.1f} anos, Média: {avg_age:.1f} anos.")
    except Exception as e: st.error(f"Erro ao gerar distribuição de idades: {e}")

@st.cache_data(show_spinner="Gerando gráfico por faixa etária...")
def plot_contagem_faixa_etaria_sql(filtros, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return
    where_clause, params = build_where_clause(filtros)
    query = f"SELECT faixa_etaria, COUNT(*) AS count FROM {tabela} {where_clause} AND faixa_etaria IS NOT NULL AND faixa_etaria != 'Desconhecida' GROUP BY faixa_etaria ORDER BY faixa_etaria;"
    try:
        df_faixa_counts = conn.execute(query, params).fetchdf()
        if df_faixa_counts.empty:
            st.info("Não há dados de faixa etária para exibir o gráfico.")
            return
        fig = px.bar(df_faixa_counts, x='count', y='faixa_etaria', orientation='h', text_auto='.2s', labels={'count': 'Nº de Prescrições', 'faixa_etaria': 'Faixa Etária'})
        fig.update_layout(title_text='Contagem de Prescrições por Faixa Etária', title_font_size=16, title_x=0.5, yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e: st.error(f"Erro ao gerar contagem por faixa etária: {e}")

@st.cache_data(show_spinner="Gerando comparativo anual...")
def plot_comparativo_sql(filtros, group_by_col, title, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None: return
    where_clause, params = build_where_clause(filtros, exclude_filters=['ano'])
    if group_by_col == 'Total':
        query = f'SELECT ano, SUM(quantidade_vendida) AS "Valor" FROM {tabela} {where_clause} AND ano IN (2019, 2020) AND quantidade_vendida IS NOT NULL GROUP BY ano;'
        x_axis, y_axis, color_axis, barmode = 'Ano', 'Valor', None, 'relative'
    else:
        if group_by_col == 'principio_ativo' and not filtros.get('principio_ativo'):
            st.info("Selecione um ou mais princípios ativos no filtro lateral para ver este comparativo.")
            return
        query = f'SELECT ano, "{group_by_col}", COUNT(*) AS "Valor" FROM {tabela} {where_clause} AND ano IN (2019, 2020) AND "{group_by_col}" IS NOT NULL GROUP BY ano, "{group_by_col}" ORDER BY "{group_by_col}", ano;'
        x_axis, y_axis, color_axis, barmode = group_by_col, 'Valor', 'Ano', 'group'
    try:
        df_comparativo = conn.execute(query, params).fetchdf()
        if df_comparativo.empty:
            st.warning(f"Dados insuficientes para o comparativo por '{group_by_col}'.")
            return
        df_comparativo['Ano'] = df_comparativo['ano'].astype(str)
        fig = px.bar(df_comparativo, x=x_axis, y=y_axis, color=color_axis, barmode=barmode, title=title, text_auto=True)
        fig.update_layout(title_font_size=16, title_x=0.5, legend_title_text='Ano')
        if group_by_col == 'Total':
            fig.update_traces(marker_color=['#1f77b4', '#ff7f0e'])
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e: st.error(f"Erro ao gerar o gráfico comparativo para '{group_by_col}': {e}")

def criar_filtros_exploracao(df_anos_para_opcoes):
    st.sidebar.header("Filtros da Exploração")
    st.sidebar.markdown("---")
    anos_disponiveis_raw = df_anos_para_opcoes['ano'].dropna().unique()
    anos_disponiveis_str = sorted([str(int(a)) for a in anos_disponiveis_raw])
    faixas_disponiveis = carregar_opcoes_filtro_do_db("faixa_etaria")
    municipios_disponiveis = carregar_opcoes_filtro_do_db("nome_municipio", add_todos=True)
    opcoes_pa = carregar_opcoes_filtro_do_db("principio_ativo")
    filtros = {
        'ano': st.sidebar.multiselect(
            "Ano:",
            options=anos_disponiveis_str,
            default=anos_disponiveis_str,
            help="Selecione os anos para análise. O comparativo anual foca em 2019 e 2020."
        ),
        'faixa_etaria': st.sidebar.multiselect(
            "Faixa Etária:",
            options=faixas_disponiveis,
            default=[],
            help="Filtre por faixas etárias específicas."
        ),
        'municipio': st.sidebar.selectbox(
            "Município:",
            options=municipios_disponiveis,
            help="Selecione um município específico. 'Todos' para análise geral."
        ),
        'principio_ativo': st.sidebar.multiselect(
            "Princípios Ativos:",
            options=opcoes_pa,
            default=[],
            help="Escolha um ou mais princípios ativos. Essencial para o comparativo por PA."
        )
    }
    st.sidebar.markdown("---")
    if st.sidebar.button("Salvar Visão Atual", use_container_width=True):
        st.success("Funcionalidade de salvar visão ainda em desenvolvimento.")
    if st.sidebar.button("Exportar Dados Filtrados", use_container_width=True):
        st.info("Funcionalidade de exportação será disponibilizada em breve.")
    st.sidebar.markdown("---")
    st.sidebar.info("Dica: Utilize os filtros para segmentar a análise e gerar insights personalizados.")
    return filtros

# --- Função de Insights Automáticos ---

def gerar_insights_automaticos(filtros, tabela=TABLE_NAME):
    conn = get_duckdb_connection()
    if conn is None:
        return ["Não foi possível conectar ao banco de dados."]
    insights = []
    where_clause, params = build_where_clause(filtros)
    # 1. Maior município em prescrições
    try:
        q1 = f"SELECT nome_municipio, COUNT(*) as total FROM {tabela} {where_clause} AND nome_municipio IS NOT NULL GROUP BY nome_municipio ORDER BY total DESC LIMIT 1;"
        r1 = conn.execute(q1, params).fetchone()
        if r1:
            insights.append(f"**Município com maior volume de prescrições:** {r1[0]} ({int(r1[1]):,} prescrições).")
    except: pass
    # 2. Princípio ativo com maior crescimento (2019 vs 2020)
    try:
        q2 = f"""
            SELECT principio_ativo,
                   SUM(CASE WHEN ano=2019 THEN 1 ELSE 0 END) as total_2019,
                   SUM(CASE WHEN ano=2020 THEN 1 ELSE 0 END) as total_2020
            FROM {tabela} {where_clause} AND principio_ativo IS NOT NULL AND ano IN (2019,2020)
            GROUP BY principio_ativo
            HAVING total_2019 > 0
            ORDER BY (CAST(total_2020 AS FLOAT) - total_2019) / total_2019 DESC
            LIMIT 1;
        """
        r2 = conn.execute(q2, params).fetchone()
        if r2 and r2[1] > 0:
            crescimento = ((r2[2] - r2[1]) / r2[1]) * 100 if r2[1] else 0
            insights.append(f"**Maior crescimento relativo de prescrições (2019→2020):** {r2[0]} (+{crescimento:.1f}%).")
    except: pass
    # 3. Faixa etária predominante
    try:
        q3 = f"SELECT faixa_etaria, COUNT(*) as total FROM {tabela} {where_clause} AND faixa_etaria IS NOT NULL GROUP BY faixa_etaria ORDER BY total DESC LIMIT 1;"
        r3 = conn.execute(q3, params).fetchone()
        if r3:
            insights.append(f"**Faixa etária predominante:** {r3[0]} ({int(r3[1]):,} prescrições).")
    except: pass
    # 4. Tendência de alta/baixa no último ano disponível
    try:
        q4 = f"SELECT ano, COUNT(*) as total FROM {tabela} {where_clause} AND ano IS NOT NULL GROUP BY ano ORDER BY ano DESC LIMIT 2;"
        df_anos = pd.DataFrame(conn.execute(q4, params).fetchall(), columns=['ano', 'total'])
        if len(df_anos) == 2:
            diff = df_anos.iloc[0]['total'] - df_anos.iloc[1]['total']
            perc = (diff / df_anos.iloc[1]['total'])*100 if df_anos.iloc[1]['total'] else 0
            tendencia = "aumento" if diff > 0 else "redução"
            insights.append(f"**Tendência anual:** {tendencia} de {abs(diff):,} prescrições ({perc:.1f}%) de {int(df_anos.iloc[1]['ano'])} para {int(df_anos.iloc[0]['ano'])}.")
    except: pass
    if not insights:
        insights.append("Nenhum insight relevante encontrado para os filtros atuais.")
    return insights

# --- Início da Página de Exploração (Layout Remodelado) ---

st.title("Exploração Interativa dos Dados de Prescrições")
st.markdown(
    """
    Esta página permite a **exploração visual detalhada** dos dados de prescrições de medicamentos controlados. 
    Utilize os filtros na barra lateral para **segmentar a análise** e descobrir insights sobre o consumo e padrões demográficos. 
    Cada visualização foi desenhada para facilitar a **identificação rápida de tendências** e características dos dados.
    """
)
st.markdown("---")

if 'df_principal' not in st.session_state or st.session_state.df_principal.empty:
    st.error("Os dados principais não foram carregados. Por favor, retorne à página inicial ou recarregue o aplicativo para carregar os dados necessários.")
    st.stop()

# Criar filtros
filtros = criar_filtros_exploracao(st.session_state.df_principal)

# Métricas de Visão Geral
total_registros, municipios_unicos, principios_unicos = get_visao_geral_metricas(filtros)

st.header("Visão Geral dos Dados Filtrados")
st.caption("As métricas abaixo são atualizadas dinamicamente de acordo com os filtros selecionados, oferecendo um panorama inicial do volume e diversidade dos dados em análise.")

cols = st.columns(3)
cols[0].metric("Total de Registros Filtrados", f"{total_registros:,}", help="Total de prescrições encontradas com os filtros selecionados.")
cols[1].metric("Municípios Únicos", f"{municipios_unicos:,}", help="Número de municípios distintos presentes nos dados filtrados.")
cols[2].metric("Princípios Ativos Únicos", f"{principios_unicos:,}", help="Quantidade de princípios ativos diferentes nas prescrições filtradas.")

st.markdown("---")

# Abas para organização do conteúdo
tab_tendencias, tab_distribuicoes, tab_comparativo, tab_detalhes = st.tabs([
    "📈 Tendências Gerais", "📊 Distribuições", "🆚 Comparativo Anual", "📋 Dados Detalhados"
])

with tab_tendencias:
    st.subheader("Análise de Tendências e Padrões de Consumo")
    st.markdown("Esta seção apresenta a evolução temporal das vendas e os princípios ativos mais prescritos, destacando padrões importantes.")
    if total_registros > 0:
        with st.container(border=True):
            st.markdown("#### Evolução Mensal da Quantidade Vendida")
            st.caption("Acompanhe o volume total de medicamentos vendidos ao longo dos meses, identificando períodos de alta ou baixa demanda.")
            plot_evolucao_temporal_sql(filtros)
        st.markdown("---")
        with st.container(border=True):
            st.markdown("#### Top 10 Princípios Ativos Mais Prescritos")
            st.caption("Descubra quais princípios ativos são os mais demandados, oferecendo insights sobre as necessidades de tratamento predominantes.")
            plot_top_principios_sql(filtros)
    else:
        st.info("Nenhum dado encontrado com os filtros selecionados para exibir tendências. Por favor, ajuste os filtros na barra lateral.")

with tab_distribuicoes:
    st.subheader("Caracterização Demográfica das Prescrições")
    st.markdown("Explore a distribuição das prescrições por idade e faixa etária, revelando o perfil dos pacientes.")
    if total_registros > 0:
        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown("#### Distribuição por Idade")
                st.caption("Histograma mostrando a distribuição das idades dos pacientes, com medidas de tendência central.")
                plot_distribuicao_idades_sql(filtros)
        with col2:
            with st.container(border=True):
                st.markdown("#### Contagem por Faixa Etária")
                st.caption("Número total de prescrições agrupadas por faixas etárias definidas, para uma visão segmentada.")
                plot_contagem_faixa_etaria_sql(filtros)
    else:
        st.info("Nenhum dado encontrado com os filtros selecionados para exibir distribuições. Por favor, ajuste os filtros na barra lateral.")

with tab_comparativo:
    st.subheader("Comparativo de Desempenho Anual (2019 vs. 2020)")
    st.markdown("Analise as variações entre os anos de 2019 e 2020 para quantidade vendida, faixas etárias e princípios ativos, identificando mudanças significativas.")
    if total_registros > 0:
        with st.container(border=True):
            st.markdown("#### Comparativo da Quantidade Total Vendida")
            st.caption("Comparação do volume total de vendas entre os anos, útil para análises de crescimento ou declínio.")
            plot_comparativo_sql(filtros, group_by_col='Total', title='Comparativo da Quantidade Total Vendida')
        st.markdown("---")
        with st.container(border=True):
            st.markdown("#### Comparativo de Prescrições por Faixa Etária")
            st.caption("Variação na distribuição das prescrições entre as faixas etárias nos anos selecionados.")
            plot_comparativo_sql(filtros, group_by_col='faixa_etaria', title='Comparativo de Prescrições por Faixa Etária')
        st.markdown("---")
        with st.container(border=True):
            st.markdown("#### Comparativo de Prescrições por Princípio Ativo")
            st.caption("Compare a performance de princípios ativos específicos entre os anos. *Selecione os princípios ativos desejados no filtro lateral.*")
            plot_comparativo_sql(filtros, group_by_col='principio_ativo', title='Comparativo de Prescrições por Princípio Ativo')
    else:
        st.info("Nenhum dado encontrado com os filtros selecionados para exibir comparativos anuais. Por favor, ajuste os filtros na barra lateral.")

with tab_detalhes:
    st.subheader("Amostra dos Dados Filtrados")
    st.markdown("Visualize uma amostra dos registros que correspondem aos seus filtros, permitindo uma inspeção direta dos dados brutos.")
    df_amostra = fetch_sample_data_from_duckdb(filtros, limit=5000)
    if not df_amostra.empty:
        st.caption(f"Exibindo uma amostra de até {len(df_amostra):,} registros que correspondem aos seus filtros. Para ver as estatísticas descritivas, expanda a seção abaixo.")
        with st.expander("Ver Estatísticas Descritivas da Amostra"):
            st.dataframe(df_amostra.describe(include='all').style.format(precision=2, na_rep="-"))
        st.dataframe(df_amostra)
    else:
        st.info("Nenhuma amostra de dados detalhados para exibir com os filtros selecionados. Por favor, ajuste os filtros na barra lateral.")

# Painel de Insights e Storytelling Automático
with st.container(border=True):
    st.subheader("Insights Automáticos")
    insights = gerar_insights_automaticos(filtros)
    for i in insights:
        st.markdown(f"- {i}")

st.markdown("---")
st.caption("Dashboard modelado seguindo padrões internacionais de UX para dashboards analíticos, estratégicos, táticos e operacionais. Utilize os filtros e compartilhe suas visões para apoiar a tomada de decisão em saúde.")
