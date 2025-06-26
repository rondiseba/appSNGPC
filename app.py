# app.py (Principal)
import streamlit as st
import pandas as pd
from src.infra.repositorio_dados import carregar_dados_processados_sngpc
from src.utils.ui_utils import svg_to_data_uri, SVG_ICONS, base64

# --- Ícones SVG Minimalistas (codificados em base64 para incorporar em HTML) ---
# Função para carregar e codificar SVGs (ou podemos embutir diretamente se forem simples)
def get_image_as_base64(path):
    with open(path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

# Como não podemos criar arquivos, vamos embutir o código SVG diretamente.
# Função para formatar SVG para uso em HTML
def svg_to_data_uri(svg_string):
    encoded_svg = base64.b64encode(svg_string.encode('utf-8')).decode('utf-8')
    return f"data:image/svg+xml;base64,{encoded_svg}"

# Dicionário de SVGs para os ícones
SVG_ICONS = {
    "sobre": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line>
    </svg>""",
    "navegacao": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 22s-8-4.5-8-11.8A8 8 0 0 1 12 2a8 8 0 0 1 8 9.8c0 7.3-8 11.8-8 11.8z"></path><circle cx="12" cy="10" r="3"></circle>
    </svg>""",
    "exploracao": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line>
    </svg>""",
    "estatisticas": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 20V10M18 20V4M6 20V16"></path>
    </svg>""",
    "clusters": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
        <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline><line x1="12" y1="22.08" x2="12" y2="12"></line>
    </svg>""",
    "previsao": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline><path d="M18 12h2a2 2 0 0 1 2 2v0a2 2 0 0 1-2 2h-2"></path>
    </svg>""",
    "anomalias": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line>
    </svg>""",
    "tecnologias": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"></path>
    </svg>""",
    "dados": """
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#2c3e50" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
    </svg>"""
}

# --- Funções de Configuração e Carregamento (sem alterações) ---
def configurar_pagina_global():
    st.set_page_config(
        page_title="Análise SNGPC",
        layout="wide",
        page_icon="💊" # O favicon pode ser um emoji, é discreto
    )
    # Estilo CSS aprimorado
    st.markdown("""
        <style>
            .main { background-color: #f8f9fa; }
            h1 {
                font-family: 'sans-serif';
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 0.2em;
            }
            h2 {
                font-family: 'sans-serif';
                color: #2c3e50;
                border-bottom: 2px solid #ecf0f1;
                padding-bottom: 0.2em;
            }
            h3, h4, h5, h6 {
                font-family: 'sans-serif';
                color: #34495e;
            }
            .st-emotion-cache-16txtl3 {
                padding-top: 2rem;
            }
            .section-header {
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .icon {
                width: 28px;
                height: 28px;
            }
            .nav-item {
                display: flex;
                align-items: center;
                gap: 10px;
                padding-bottom: 8px;
            }
            .nav-icon {
                 width: 20px;
                 height: 20px;
            }
        </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=3600, show_spinner="Carregando dados principais do banco de dados...")
def carregar_dados_para_sessao():
    try:
        df = carregar_dados_processados_sngpc() 
        if df.empty:
            st.error("Nenhum dado encontrado para os anos de 2019 e 2020 no banco de dados.")
            return pd.DataFrame()
        
        if 'data' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['data']):
             df['data'] = pd.to_datetime(df['data'], errors='coerce')
        
        if 'ano' not in df.columns:
            st.error("ERRO CRÍTICO: Coluna 'ano' não presente nos dados carregados.")
            return pd.DataFrame()
        elif not pd.api.types.is_integer_dtype(df['ano']):
             df['ano'] = pd.to_numeric(df['ano'], errors='coerce').astype('Int64')
        
        return df
        
    except FileNotFoundError as e:
        st.error(f"ERRO CRÍTICO AO ACESSAR O BANCO DE DADOS: {e}")
        st.info("Verifique se o arquivo do banco de dados DuckDB ('sngpc_analytics.duckdb') existe na pasta 'dados/' e se o script de ingestão ('ingest_to_duckdb.py') foi executado corretamente.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao carregar dados da sessão: {type(e).__name__} - {str(e)}")
        return pd.DataFrame()

# --- Ponto de Entrada Principal da Aplicação ---
configurar_pagina_global()

if 'df_principal' not in st.session_state:
    st.session_state.df_principal = carregar_dados_para_sessao()

# --- Conteúdo da Página Principal (Refatorado com Design Clean) ---
if 'df_principal' not in st.session_state or st.session_state.df_principal.empty:
    st.error("Os dados principais não puderam ser carregados. Verifique os logs e mensagens acima.")
    st.caption("Certifique-se de que o banco de dados DuckDB foi criado, populado corretamente (com dados para 2019-2020) e está acessível.")
else:
    # Título e Autor
    st.title("Análise de Prescrição de Medicamentos Controlados (SNGPC)")
    st.markdown("##### *Projeto de TCC em Análise de Dados e IA UAB UFMA*")
    st.markdown("**Desenvolvido por:** Rondineli Seba Salomão - *Bacharel em Ciência da Computação e Farmácia-Bioquímica*")
    st.markdown("---")

    # Seção Sobre o Projeto
    st.markdown(f"""
        <div class="section-header">
            <img class="icon" src="{svg_to_data_uri(SVG_ICONS['sobre'])}">
            <h2>Sobre o Projeto</h2>
        </div>
    """, unsafe_allow_html=True)
    st.markdown(
        """
        Este dashboard interativo é o resultado do meu Trabalho de Conclusão de Curso (TCC) que aplica técnicas de 
        Inteligência Artificial e Análise de Dados para explorar o vasto conjunto de informações do **Sistema Nacional 
        de Gerenciamento de Produtos Controlados (SNGPC)**. 
        
        O objetivo principal é fornecer uma ferramenta analítica que permita a profissionais de saúde, gestores e pesquisadores 
        extrair insights valiosos, monitorar tendências e identificar padrões incomuns na dispensação de medicamentos controlados no Brasil, 
        com um foco especial no período pré e durante a pandemia de COVID-19 (2019-2020).
        """
    )
    st.markdown("<br>", unsafe_allow_html=True) # Espaçamento

    # Seção de Navegação
    st.markdown(f"""
        <div class="section-header">
            <img class="icon" src="{svg_to_data_uri(SVG_ICONS['navegacao'])}">
            <h2>Como Navegar no Dashboard</h2>
        </div>
    """, unsafe_allow_html=True)
    st.info("Utilize o menu na barra lateral à esquerda para acessar as diferentes páginas de análise:")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Análises Exploratórias")
        st.markdown(
            f"""
            <div class="nav-item">
                <img class="nav-icon" src="{svg_to_data_uri(SVG_ICONS['exploracao'])}">
                <span><b>Exploração Interativa:</b> Filtre e visualize os dados de forma dinâmica.</span>
            </div>
            <div class="nav-item">
                <img class="nav-icon" src="{svg_to_data_uri(SVG_ICONS['estatisticas'])}">
                <span><b>Análise Estatística:</b> Explore distribuições e realize testes estatísticos.</span>
            </div>
            """, unsafe_allow_html=True
        )
    
    with col2:
        st.subheader("Modelagem com Machine Learning")
        st.markdown(
            f"""
            <div class="nav-item">
                <img class="nav-icon" src="{svg_to_data_uri(SVG_ICONS['clusters'])}">
                <span><b>Análise de Clusters:</b> Identifique grupos de prescrições com características similares.</span>
            </div>
            <div class="nav-item">
                <img class="nav-icon" src="{svg_to_data_uri(SVG_ICONS['previsao'])}">
                <span><b>Previsão de Séries Temporais:</b> Preveja a demanda futura de medicamentos.</span>
            </div>
            <div class="nav-item">
                <img class="nav-icon" src="{svg_to_data_uri(SVG_ICONS['anomalias'])}">
                <span><b>Detecção de Anomalias:</b> Encontre registros de dispensação atípicos.</span>
            </div>
            """, unsafe_allow_html=True
        )
    st.markdown("---")
    
    # Seção de Metodologia em um Expander
    with st.expander("Expandir para ver Metodologia, Tecnologias e Fonte dos Dados"):
        st.markdown(f"""
            <div class="section-header">
                <img class="icon" src="{svg_to_data_uri(SVG_ICONS['tecnologias'])}">
                <h3>Metodologia e Tecnologias</h3>
            </div>
        """, unsafe_allow_html=True)
        st.markdown(
            """
            O projeto foi desenvolvido em **Python**, com um pipeline ETL para garantir performance e escalabilidade.
            - **Gerenciamento de Dados:** **DuckDB**, um banco de dados analítico de alta performance.
            - **Dashboard Interativo:** Construído com **Streamlit**.
            - **Manipulação de Dados:** **Pandas** e **NumPy**.
            - **Visualizações:** Gráficos interativos gerados com **Plotly Express**.
            - **Modelagem Estatística e ML:** **SciPy**, **Statsmodels** e **Scikit-learn**.
            """
        )
        st.markdown("<br>", unsafe_allow_html=True) # Espaçamento
        
        st.markdown(f"""
            <div class="section-header">
                <img class="icon" src="{svg_to_data_uri(SVG_ICONS['dados'])}">
                <h3>Fonte dos Dados</h3>
            </div>
        """, unsafe_allow_html=True)
        st.markdown(
            """
            Os dados foram extraídos da plataforma [Base dos Dados](https://basedosdados.org/), que disponibiliza publicamente 
            as informações do SNGPC da ANVISA. O escopo desta análise compreende:
            - **Período:** Anos de 2019 e 2020.
            - **Abrangência:** Capitais Brasileiras.
            - **Volume Processado:** Mais de 21 milhões de registros.
            """
        )

# Configuração da Barra Lateral (Sidebar)
st.sidebar.header("Dashboard SNGPC")
st.sidebar.markdown(f"Versão: 1.0 | {pd.Timestamp.now(tz='America/Sao_Paulo').strftime('%d/%m/%Y')}")
st.sidebar.markdown("---")