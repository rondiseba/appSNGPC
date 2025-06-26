# Este módulo contém funções utilitárias para conectar e consultar o banco de dados DuckDB,
import streamlit as st
import duckdb
from pathlib import Path
import pandas as pd

# --- Configurações e Constantes Compartilhadas ---
# BASE_DIR agora é definido a partir da localização deste arquivo em src/utils/
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DUCKDB_FILE_PATH = BASE_DIR / "dados" / "sngpc_analytics.duckdb"
TABLE_NAME = "prescricoes"
TABLE_MAPEAMENTO = "mapeamento_controlados"
TABLE_ATC = "mapeamento_atc"
TABLE_MUNICIPIOS = "mapeamento_municipios"

# --- Funções de Conexão ---

@st.cache_resource(show_spinner="Conectando ao banco de dados...")
def get_duckdb_connection():
    """
    Estabelece e retorna uma conexão read-only com o DuckDB para o app Streamlit.
    """
    try:
        return duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=True)
    except Exception as e:
        st.exception(f"Erro ao conectar ao DuckDB para o app: {e}")
        return None

def get_db_connection_for_etl():
    """
    Cria e retorna uma conexão de LEITURA/ESCRITA com o DuckDB,
    específica para scripts de ETL, sem usar o cache do Streamlit.
    """
    try:
        return duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=False)
    except Exception as e:
        print(f"FATAL: Erro ao conectar ao DB para ETL: {e}")
        return None

# --- Funções de Query Compartilhadas ---

def build_where_clause(filtros, exclude_filters=None):
    """
    Constrói a cláusula WHERE e a lista de parâmetros para SQL dinamicamente.
    Esta é a versão completa, que lida com todos os filtros do dashboard.
    """
    if exclude_filters is None:
        exclude_filters = []
        
    conditions = ["ano IN (2019, 2020)"]
    params = []

    # Condição para o filtro de Ano
    if 'ano' not in exclude_filters and filtros.get('ano'):
        try:
            anos_int = [int(a) for a in filtros['ano']]
            if anos_int:
                placeholders = ', '.join(['?'] * len(anos_int))
                conditions.append(f"ano IN ({placeholders})")
                params.extend(anos_int)
        except ValueError:
            st.warning("Valor inválido para filtro de ano, ignorado na query.")
            
    # Condição para o filtro de Faixa Etária
    if 'faixa_etaria' not in exclude_filters and filtros.get('faixa_etaria'):
        placeholders = ', '.join(['?'] * len(filtros['faixa_etaria']))
        conditions.append(f"faixa_etaria IN ({placeholders})")
        params.extend(filtros['faixa_etaria'])

    # Condição para o filtro de Município
    if 'municipio' not in exclude_filters and filtros.get('municipio') and filtros['municipio'] != 'Todos':
        conditions.append(f"nome_municipio = ?")
        params.append(filtros['municipio'])

    # Condição para o filtro de Princípio Ativo
    if 'principio_ativo' not in exclude_filters and filtros.get('principio_ativo'):
        placeholders = ', '.join(['?'] * len(filtros['principio_ativo']))
        conditions.append(f"principio_ativo IN ({placeholders})")
        params.extend(filtros['principio_ativo'])
        
    where_clause = f"WHERE {' AND '.join(conditions)}"
    return where_clause, params

@st.cache_data(show_spinner="Carregando opções de filtro...")
def carregar_opcoes_filtro_do_db(coluna_filtro, tabela=TABLE_NAME, add_todos=False, placeholder_todos="Todos"):
    """Busca valores distintos de uma coluna no DuckDB para popular filtros."""
    conn = get_duckdb_connection()
    if conn is None: return [placeholder_todos] if add_todos else []
    
    query = f'SELECT DISTINCT "{coluna_filtro}" FROM {tabela} WHERE "{coluna_filtro}" IS NOT NULL AND ano IN (2019, 2020) ORDER BY "{coluna_filtro}" ASC;'
    try:
        options = conn.execute(query).df()[coluna_filtro].tolist()
    except Exception as e:
        st.warning(f"Não foi possível carregar opções para '{coluna_filtro}': {e}")
        options = []
        
    return [placeholder_todos] + sorted(options) if add_todos else sorted(options)

@st.cache_data(show_spinner="Carregando opções de previsão...")
def carregar_opcoes_previsao(tabela=TABLE_NAME):
    """
    Retorna duas listas: princípios ativos e municípios distintos da tabela.
    """
    conn = get_duckdb_connection()
    if conn is None:
        return [], []
    try:
        opcoes_pa = conn.execute(f'SELECT DISTINCT principio_ativo FROM {tabela} WHERE principio_ativo IS NOT NULL ORDER BY principio_ativo ASC;').df()['principio_ativo'].tolist()
        opcoes_mun = conn.execute(f'SELECT DISTINCT nome_municipio FROM {tabela} WHERE nome_municipio IS NOT NULL ORDER BY nome_municipio ASC;').df()['nome_municipio'].tolist()
    except Exception as e:
        st.warning(f"Não foi possível carregar opções de previsão: {e}")
        opcoes_pa, opcoes_mun = [], []
    return opcoes_pa, opcoes_mun