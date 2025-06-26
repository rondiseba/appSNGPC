# scripts para executar o ETL e carregar mapeamento
from pathlib import Path
import duckdb
import pandas as pd
import sys
from unidecode import unidecode
import logging
import time

# Adiciona a pasta raiz ao caminho do Python
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.utils.database_utils import get_db_connection_for_etl, DUCKDB_FILE_PATH, TABLE_NAME, TABLE_MAPEAMENTO, TABLE_ATC, TABLE_MUNICIPIOS

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

def validar_dataframe(df):
    # Exemplo: checar colunas obrigatórias
    colunas_obrigatorias = ['ano', 'mes', 'principio_ativo'] # ...adicione outras se necessário
    for col in colunas_obrigatorias:
        if col not in df.columns:
            log.error(f"Coluna obrigatória ausente: {col}")
            raise ValueError(f"Coluna obrigatória ausente: {col}")
    # Outras validações podem ser adicionadas aqui

def executar_pipeline_etl_sql(conexao, caminho_pasta_entrada):
    """
    Executa o pipeline completo de ETL usando uma abordagem híbrida robusta.
    """
    print("--- INICIANDO PIPELINE ETL (VERSÃO COM PADRONIZAÇÃO AVANÇADA) ---")
    tabela_raw = "prescricoes_raw"
    try:
        # ETAPA 0: Instalar extensões
        print("\n[ETAPA 0/9] Instalando extensões do DuckDB (icu)...")
        conexao.execute("INSTALL icu; LOAD icu;")
        print("-> Extensão 'icu' carregada.")

        # ETAPA 1: Carregar dados brutos em lotes para uma tabela de Staging
        print(f"\n[ETAPA 1/9] Carregando dados brutos para a tabela '{tabela_raw}'...")
        conexao.execute(f"DROP TABLE IF EXISTS {tabela_raw};")
        pasta_dados_brutos = Path(caminho_pasta_entrada)
        arquivos_csv = list(pasta_dados_brutos.glob('*.csv'))
        if not arquivos_csv:
            raise FileNotFoundError(f"Nenhum arquivo .csv encontrado em: {pasta_dados_brutos}")
        
        print(f"Encontrados {len(arquivos_csv)} arquivos para processar...")
        # Define o schema explicitamente para garantir consistência
        conexao.execute(f"CREATE TABLE {tabela_raw} (ano VARCHAR, mes VARCHAR, sigla_uf VARCHAR, id_municipio VARCHAR, principio_ativo VARCHAR, descricao_apresentacao VARCHAR, quantidade_vendida VARCHAR, unidade_medida VARCHAR, conselho_prescritor VARCHAR, sigla_uf_conselho_prescritor VARCHAR, tipo_receituario VARCHAR, cid10 VARCHAR, sexo VARCHAR, idade VARCHAR, unidade_idade VARCHAR);")
        
        tamanho_lote = 500000
        for arquivo in arquivos_csv:
            print(f"--- Lendo arquivo: {arquivo.name} ---")
            with pd.read_csv(arquivo, sep=',', low_memory=False, encoding='latin1', chunksize=tamanho_lote, dtype=str) as leitor:
                for i, lote_df in enumerate(leitor):
                    print(f" - Processando lote {i+1} ({len(lote_df):,} linhas)...")
                    lote_df.columns = [col.lower().strip().replace(' ', '_') for col in lote_df.columns]
                    conexao.execute(f"INSERT INTO {tabela_raw} BY NAME SELECT * FROM lote_df;")
        
        total_bruto = conexao.execute(f"SELECT COUNT(*) FROM {tabela_raw}").fetchone()[0]
        print(f"-> {total_bruto:,} registros brutos carregados com sucesso.")

        # ETAPA 2: Padronização Avançada de Princípios Ativos (direto na tabela raw)
        print("\n[ETAPA 2/9] Padronizando nomes de princípios ativos com Regex e Unaccent...")
        conexao.execute(f"UPDATE {tabela_raw} SET principio_ativo = upper(strip_accents(trim(principio_ativo)));")
        
        # CORREÇÃO DE SINTAXE: Adicionado o ']' para fechar a lista
        termos = [
            'CLORIDRATO DE', 'BROMIDRATO DE', 'FOSFATO DE', 'ACETATO DE', 'DECANOATO DE', 'NITRATO DE',
            'HEMISSULFATO DE', 'SUCCINATO DE', 'MALEATO DE', 'MESILATO DE', 'VALERATO DE', 'ESILATO DE',
            'CIPIONATO DE', 'TRI-HIDRATADO', 'TRIHIDRATADA', 'MONOIDRATADO', 'DI-HIDRATADO', 'ANIDRO', 'OXALATO DE',
            'SULFATO DE', 'CLORIDRATO', 'BROMIDRATO', 'FOSFATO', 'ACETATO', 'DECANOATO', 'NITRATO', 'SESQUI-HIDRATADO',
            'UNDECILATO DE', 'DIPROPIONATO DE BETAMETASONA','CLORETO DE BENZALCONIO',
        ]

        for termo in termos:
            conexao.execute(f"UPDATE {tabela_raw} SET principio_ativo = trim(replace(principio_ativo, '{termo}', ''));")
        
        # Padroniza múltiplos espaços e o sinal de '+'
        conexao.execute(f"UPDATE {tabela_raw} SET principio_ativo = regexp_replace(principio_ativo, '\\s*\\+\\s*', ' + ', 'g');")
        conexao.execute(f"UPDATE {tabela_raw} SET principio_ativo = trim(regexp_replace(principio_ativo, '\\s+', ' ', 'g'));")
        
        print(" - Criando a coluna 'join_key' na tabela raw...")
        conexao.execute(f"ALTER TABLE {tabela_raw} ADD COLUMN IF NOT EXISTS join_key VARCHAR;")
        conexao.execute(f"UPDATE {tabela_raw} SET join_key = principio_ativo;")
        print("-> Princípios ativos padronizados.")

        # ETAPA 2.5: Preparar tabela de mapeamento ATC para o JOIN (Lógica movida da ETAPA 6)
        print("\n[ETAPA 2.5/9] Preparando a tabela de mapeamento ATC para consistência...")
        conexao.execute(f"ALTER TABLE {TABLE_ATC} ADD COLUMN IF NOT EXISTS join_key VARCHAR;")
        
        # Aplica a mesma lógica de padronização da ETAPA 2 à tabela ATC
        conexao.execute(f"UPDATE {TABLE_ATC} SET principio_ativo = upper(strip_accents(trim(principio_ativo)));")
        
        for termo in termos:
            conexao.execute(f"UPDATE {TABLE_ATC} SET principio_ativo = trim(replace(principio_ativo, '{termo}', ''));")
        
        conexao.execute(f"UPDATE {TABLE_ATC} SET principio_ativo = regexp_replace(principio_ativo, '\\s*\\+\\s*', ' + ', 'g');")
        conexao.execute(f"UPDATE {TABLE_ATC} SET principio_ativo = trim(regexp_replace(principio_ativo, '\\s+', ' ', 'g'));")
        
        # Cria a join_key final na tabela de mapeamento
        conexao.execute(f"UPDATE {TABLE_ATC} SET join_key = principio_ativo;")
        print("-> Tabela de mapeamento ATC padronizada e com join_key criada.")

        # ETAPA 3: Criar tabela final com transformações, tipos corretos e junção
        print(f"\n[ETAPA 3/9] Criando tabela final '{TABLE_NAME}' com transformações e enriquecimento...")
        conexao.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        regex_dosagem = r'(\d+\.?\d*\s?(?:MG/ML|MG/G|MG|MCG|UI|G|ML))'
        
        # CORREÇÃO DA DUPLICIDADE: Os joins com as tabelas de mapeamento agora usam subconsultas
        # com ROW_NUMBER() para garantir que apenas uma correspondência seja retornada por join_key.
        conexao.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            t1.*,
            COALESCE(mun.nome_municipio, 'Desconhecido') as nome_municipio,
            COALESCE(atc.codigo_atc, 'Não Classificado') as codigo_atc,
            COALESCE(atc.classe_terapeutica, 'Não Classificada') as classe_terapeutica,
            CASE
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'COM REV|COMP REV') THEN 'Comprimido Revestido'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'COMP') THEN 'Comprimido'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'CAPS|CAP') THEN 'Cápsula'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'SOL OR|SOL') THEN 'Solução Oral'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'GTS') THEN 'Gotas'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'XPE') THEN 'Xarope'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'CREM') THEN 'Creme'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'POM') THEN 'Pomada'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'SUSP') THEN 'Suspensão'
                WHEN regexp_matches(upper(t1.descricao_apresentacao), 'INJ') THEN 'Injetável'
                ELSE 'Não Especificada'
            END as forma_farmaceutica,
            m.lista AS anvisa_lista,
            CAST(0 AS TINYINT) AS idade_modificada_flag,
            CAST(0 AS TINYINT) AS quantidade_modificada_flag,
            CAST(NULL AS VARCHAR) as faixa_etaria,
            CAST(NULL AS BOOLEAN) AS periodo_valido_controlado
        FROM (
            SELECT
                CAST(ano AS INTEGER) AS ano, CAST(mes AS INTEGER) AS mes,
                make_date(CAST(ano AS INTEGER), CAST(mes AS INTEGER), 1) AS data,
                upper(trim(sigla_uf)) as sigla_uf,
                id_municipio,
                principio_ativo,
                join_key,
                descricao_apresentacao,
                regexp_extract(descricao_apresentacao, '{regex_dosagem}', 1) as dosagem,
                CAST(try_cast(replace(quantidade_vendida, ',', '.') as DOUBLE) as DOUBLE) as quantidade_vendida,
                COALESCE(cid10, 'Não Informado') as cid10,
                CASE WHEN sexo IN ('1', '1.0') THEN 'Masculino' WHEN sexo IN ('2', '2.0') THEN 'Feminino' ELSE 'Não Informado' END as sexo,
                CAST(try_cast(idade as INTEGER) as INTEGER) as idade,
                conselho_prescritor
            FROM {tabela_raw} WHERE ano IS NOT NULL AND mes IS NOT NULL
        ) AS t1
        LEFT JOIN (
            -- Subconsulta para de-duplicar a tabela de mapeamento de controlados
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY join_key ORDER BY inclusao_lista DESC) as rn
                FROM {TABLE_MAPEAMENTO}
            ) WHERE rn = 1
        ) m ON t1.join_key = m.join_key
        LEFT JOIN (
            -- Subconsulta para de-duplicar a tabela de mapeamento ATC
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY join_key ORDER BY codigo_atc) as rn
                FROM {TABLE_ATC}
            ) WHERE rn = 1
        ) atc ON t1.join_key = atc.join_key
        LEFT JOIN {TABLE_MUNICIPIOS} mun ON t1.id_municipio = mun.id_municipio;
        """)
        print("-> Tabela processada, enriquecida e colunas criadas.")

        # ETAPA 4: Tratamento de Outliers e Flags e criação de faixa etária
        print("\n[ETAPA 4/9] Tratando outliers, flags e criando faixas etárias...")
        media_idade = conexao.execute(f"SELECT AVG(idade) FROM {TABLE_NAME} WHERE idade BETWEEN 0 AND 110").fetchone()[0]
        if media_idade is not None:
            conexao.execute(f"UPDATE {TABLE_NAME} SET idade_modificada_flag = 1, idade = {round(media_idade)} WHERE idade IS NULL OR idade < 0 OR idade > 110;")
        
        limite_superior_qtd = conexao.execute(f"SELECT quantile_cont(quantidade_vendida, 0.75) + 1.5 * (quantile_cont(quantidade_vendida, 0.75) - quantile_cont(quantidade_vendida, 0.25)) FROM {TABLE_NAME} WHERE quantidade_vendida IS NOT NULL").fetchone()[0]
        if limite_superior_qtd is not None:
            conexao.execute(f"UPDATE {TABLE_NAME} SET quantidade_modificada_flag = 1, quantidade_vendida = abs(quantidade_vendida) WHERE quantidade_vendida < 0;")
            conexao.execute(f"UPDATE {TABLE_NAME} SET quantidade_modificada_flag = 1, quantidade_vendida = {limite_superior_qtd} WHERE quantidade_vendida > {limite_superior_qtd};")
            
        conexao.execute(f"""UPDATE {TABLE_NAME} SET faixa_etaria = CASE
            WHEN idade IS NULL THEN 'Desconhecida'
            WHEN idade < 15 THEN 'Criança (0-14)'
            WHEN idade < 25 THEN 'Jovem Adulto (15-24)'
            WHEN idade < 60 THEN 'Adulto (25-59)'
            WHEN idade < 65 THEN 'Idoso (60-64)'
            ELSE 'Idoso (65+)' END;""")
        print("-> Outliers e valores ausentes tratados.")

        # ETAPA 5: Atualizar período válido para controlados
        print("\n[ETAPA 5/9] Atualizando período válido para medicamentos controlados...")
        conexao.execute(f"""
        UPDATE {TABLE_NAME}
        SET periodo_valido_controlado = CASE
            WHEN strptime(CAST(data AS VARCHAR), '%Y-%m-%d') >= strptime(m.inclusao_lista, '%d/%m/%Y')
            AND (m.exclusao_lista IS NULL OR strptime(CAST(data AS VARCHAR), '%Y-%m-%d') <= strptime(m.exclusao_lista, '%d/%m/%Y'))
            THEN TRUE ELSE FALSE END
        FROM {TABLE_MAPEAMENTO} m
        WHERE {TABLE_NAME}.principio_ativo = m.principio_ativo;
        """)
        print("-> Período válido para controlados atualizado.")

        # ETAPA 6: Enriquecimento com Classificação ATC (Etapa simplificada)
        print(f"\n[ETAPA 6/9] Verificando enriquecimento com a classificação ATC...")
        count_nulls = conexao.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE codigo_atc IS NULL OR classe_terapeutica IS NULL;").fetchone()[0]
        if count_nulls > 0:
             print(f"Aviso: {count_nulls} registros ainda sem classificação ATC. Verifique o mapeamento.")
        print("-> Verificação de dados ATC concluída.")

        # ETAPA 7: Limpeza de Tabelas Temporárias
        print("\n[ETAPA 7/9] Limpando tabelas temporárias...")
        conexao.execute(f"DROP TABLE IF EXISTS {tabela_raw};")
        print("-> Tabelas temporárias removidas.")

        # ETAPA 8: Criar Índices
        print("\n[ETAPA 8/9] Criando índices na tabela final...")
        colunas_para_indexar = ['ano', 'nome_municipio', 'principio_ativo', 'data', 'faixa_etaria', 'anvisa_lista', 'sigla_uf', 'codigo_atc', 'classe_terapeutica']
        for coluna in colunas_para_indexar:
            print(f" - Criando índice para a coluna: '{coluna}'...")
            conexao.execute(f"CREATE INDEX IF NOT EXISTS idx_{coluna} ON {TABLE_NAME} ({coluna});")
        print("-> Índices criados com sucesso.")

        # ETAPA 9: Verificação Final
        print("\n[ETAPA 9/9] Verificação final da qualidade dos dados...")
        total_final = conexao.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"-> Tabela '{TABLE_NAME}' contém {total_final:,} registros válidos.")
        resumo = conexao.execute(f"""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT principio_ativo) as principios_ativos_unicos,
            COUNT(DISTINCT nome_municipio) as municipios_unicos,
            COUNT(DISTINCT sigla_uf) as ufs_unicas
            COUNT(DISTINCT classe_terapeutica) as classes_terapeuticas_unicas
            COUNT(DISTINCT codigo_atc) as codigos_atc_unicos
            COUNT(DISTINCT faixa_etaria) as faixas_etarias_unicas
            COUNT(DISTINCT anvisa_lista) as listas_anvisa_unicas
            COUNT(DISTINCT categoria) as categorias_unicas
        FROM {TABLE_NAME}
        """).fetchone()
        print(f"""
        Resumo Final:
        - Total de registros: {resumo[0]:,}
        - Princípios ativos únicos: {resumo[1]:,}
        - Municípios únicos: {resumo[2]:,}
        - UFs únicas: {resumo[3]:,}
        - Tabela final criada com sucesso em: {TABLE_NAME}
        - Classes terapêuticas únicas: {resumo[4]:,}
        - Códigos ATC únicos: {resumo[5]:,}
        - Faixas etárias únicas: {resumo[6]:,}
        - Listas ANVISA únicas: {resumo[7]:,}
        - Categorias únicas: {resumo[8]:,}
        - Verifique os dados e índices criados.
        """)

    except FileNotFoundError as e:
        log.error(f"Arquivo não encontrado: {e}")
    except Exception as e:
        log.exception("Erro inesperado durante o ETL")
    finally:
        if conexao is not None:
            conexao.close()
            print("Conexão com DuckDB fechada.")
    
    print(f"\n--- PIPELINE ETL CONCLUÍDO ---")

def carregar_mapeamento_para_db():
    print("--- INICIANDO CARGA DO MAPEAMENTO PARA O BANCO DE DADOS ---")
    BASE_DIR = Path(__file__).resolve().parent.parent
    CAMINHO_MAPEAMENTO_CSV = BASE_DIR / "dados" / "mapeamento_Controlados.csv"
    DUCKDB_FILE_PATH = BASE_DIR / "dados" / "sngpc_analytics.duckdb"
    NOME_TABELA = "mapeamento_controlados"
    
    conexao = None
    print(f"Conectando ao banco de dados DuckDB em: {DUCKDB_FILE_PATH}")
    
    try:
        conexao = duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=False)
        print("Conexão estabelecida com sucesso.")
        
        if not CAMINHO_MAPEAMENTO_CSV.exists():
            raise FileNotFoundError(f"Arquivo de mapeamento não encontrado: {CAMINHO_MAPEAMENTO_CSV}")
        
        print(f"Lendo arquivo de mapeamento: {CAMINHO_MAPEAMENTO_CSV}")
        df_mapeamento = pd.read_csv(CAMINHO_MAPEAMENTO_CSV, sep=',')
        
        print("Carregando tabela de mapeamento ATC do banco de dados...")
        tabela_atc = "mapeamento_atc"
        df_atc = conexao.execute(f"SELECT * FROM {tabela_atc}").fetchdf()

        df_atc['join_key'] = df_atc['principio_ativo'].str.strip().str.upper()
        print("Tabela de mapeamento ATC carregada e preparada.")
        
        print("Padronizando nomes das colunas...")
        df_mapeamento.columns = [col.lower().strip().replace(' ', '_') for col in df_mapeamento.columns]
        
        if 'principio_ativo' in df_mapeamento.columns:
            print("Padronizando e criando chave de junção sem acentos...")
            df_mapeamento['principio_ativo_base'] = df_mapeamento['principio_ativo'].str.strip().str.upper()
            df_mapeamento['join_key'] = df_mapeamento['principio_ativo_base'].apply(lambda x: unidecode(str(x)).upper())
        
        print(f"Criando ou substituindo a tabela '{NOME_TABELA}'...")
        conexao.sql(f"CREATE OR REPLACE TABLE {NOME_TABELA} AS SELECT * FROM df_mapeamento")
        
        total_linhas = conexao.execute(f"SELECT COUNT(*) FROM {NOME_TABELA};").fetchone()[0]
        print(f"Tabela '{NOME_TABELA}' criada/atualizada com sucesso com {total_linhas} registros.")

    except Exception as e:
        print(f"ERRO na carga do mapeamento: {e}")
    finally:
        if conexao is not None:
            conexao.close()
            print("Conexão com DuckDB fechada.")
    
    print(f"\n--- CARGA DO MAPEAMENTO CONCLUÍDA ---")

if __name__ == '__main__':
    conexao_etl = get_db_connection_for_etl()
    if conexao_etl:
        executar_pipeline_etl_sql(
            caminho_pasta_entrada=Path.cwd() / "dados" / "dados_Originais",
            conexao=conexao_etl
        )
    
    carregar_mapeamento_para_db()
    print("\n--- CARGA DO MAPEAMENTO DE PRINCÍPIOS ATIVOS CONCLUÍDA ---")
    
    print("\n--- ETL COMPLETO ---")
    print("Verifique os logs para detalhes e possíveis avisos.")
    
executar_auditoria_completa = True
if executar_auditoria_completa:
    from scripts.auditoria_etl_bd import executar_auditoria_completa
    executar_auditoria_completa(relatorio_dir="dados/relatorios_etl")
else:
    print("Auditoria pós-ETL não executada. Defina 'executar_auditoria_completa' como True para ativar.")
    print("Você pode executar a auditoria separadamente usando o script 'auditoria_etl_bd.py'.")