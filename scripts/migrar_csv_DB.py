# O ETL agora é feito direto no banco DuckDB, sem salvar CSV intermediário.
# Envio dos dados do CSV para o DuckDB
# Este script lê um arquivo CSV e o insere em uma tabela DuckDB, criando índices para otimização de consultas.
# Certifique-se de que o DuckDB está instalado: pip install duckdb
# O arquivo CSV deve estar no diretório 'dados' com o nome 'dados_processados.csv'.
# O banco de dados DuckDB será criado no mesmo diretório com o nome 'sngpc_analytics.duckdb'.
# Este script também verifica se a tabela já existe e permite ao usuário decidir se deseja recriá-la.  

import duckdb
import pandas as pd # Apenas para inspecionar, se necessário
from pathlib import Path

# --- Configurações ---
BASE_DIR = Path(__file__).resolve().parent
CSV_FILE_PATH = BASE_DIR / "dados" / "dados_processados.csv"
DUCKDB_FILE_PATH = BASE_DIR / "dados" / "sngpc_analytics.duckdb" # Onde o arquivo do banco será salvo
TABLE_NAME = "prescricoes"

def create_duckdb_database():
    print(f"Conectando/Criando banco de dados DuckDB em: {DUCKDB_FILE_PATH}...")
    con = duckdb.connect(database=str(DUCKDB_FILE_PATH), read_only=False)

    print(f"Verificando se a tabela '{TABLE_NAME}' já existe...")
    result = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}';").fetchone()

    if result:
        print(f"A tabela '{TABLE_NAME}' já existe. Deseja recriá-la? (Isso apagará os dados existentes)")
        user_choice = input("Digite 'sim' para recriar, ou qualquer outra coisa para pular a ingestão: ").strip().lower()
        if user_choice == 'sim':
            print(f"Apagando tabela '{TABLE_NAME}' existente...")
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        else:
            print(f"Ingestão pulada pois a tabela '{TABLE_NAME}' já existe e não foi solicitado recriar.")
            con.close()
            return

    print(f"Iniciando a ingestão do arquivo CSV: {CSV_FILE_PATH} para a tabela '{TABLE_NAME}'...")
    try:
        # DuckDB é muito eficiente para ler CSVs diretamente.
        # Ele tentará inferir os tipos de dados automaticamente (read_csv_auto).
        # Se precisar de controle mais fino sobre os tipos, pode usar dtypes na query.
        # Ex: read_csv_auto('{CSV_FILE_PATH}', dtypes={'coluna1': 'VARCHAR', 'coluna2': 'INTEGER'})
        # Ou definir a tabela primeiro e depois inserir.
        
        # Opção 1: Deixar o DuckDB inferir tudo e criar a tabela
        con.execute(f"""
            CREATE TABLE {TABLE_NAME} AS 
            SELECT * FROM read_csv_auto('{str(CSV_FILE_PATH)}', header=True, sample_size=-1);
        """)
        # sample_size=-1 para usar todo o arquivo para inferência de tipo (pode ser mais lento, mas mais preciso)
        # ou um valor positivo como 204800 para amostrar N linhas.
        
        print(f"Dados ingeridos com sucesso na tabela '{TABLE_NAME}'.")
        
        total_rows = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};").fetchone()[0]
        print(f"Total de linhas na tabela '{TABLE_NAME}': {total_rows:,}")

        print("Inspecionando o esquema da tabela criada:")
        schema = con.execute(f"DESCRIBE {TABLE_NAME};").fetchall()
        for col_name, col_type, _, _, _, _ in schema:
            print(f"- Coluna: {col_name}, Tipo Inferido: {col_type}")

        # --- Criar Índices (CRUCIAL para performance de consulta) ---
        # Ajuste os nomes das colunas conforme o seu CSV.
        # Estes são exemplos baseados nas colunas que temos discutido.
        colunas_para_indexar = [
            'ano', 
            'nome_municipio', 
            'principio_ativo', 
            'data', # Se a coluna 'data' for usada em filtros de range
            'faixa_etaria',
            'sexo' 
            # Adicione outras colunas frequentemente usadas em cláusulas WHERE ou JOINs
        ]
        
        print("\nCriando índices...")
        for coluna in colunas_para_indexar:
            # Verifica se a coluna existe antes de tentar criar o índice
            # (DESCRIBE não é padrão SQL para checar coluna, PRAGMA table_info é mais SQLite)
            # DuckDB não falha ao criar índice em coluna inexistente, mas é bom verificar logicamente.
            # Para DuckDB, podemos checar na lista de colunas do schema.
            if any(col_name == coluna for col_name, _, _, _, _, _ in schema):
                print(f"Criando índice na coluna '{coluna}'...")
                try:
                    con.execute(f"CREATE INDEX idx_{coluna} ON {TABLE_NAME} ({coluna});")
                    print(f"Índice 'idx_{coluna}' criado com sucesso.")
                except Exception as e_index:
                    print(f"Falha ao criar índice em '{coluna}': {e_index}")
            else:
                print(f"Coluna '{coluna}' não encontrada no schema, pulando criação de índice.")
        
        print("\nOtimização do banco de dados (VACUUM e ANALYZE)...")
        con.execute("VACUUM;") # Opcional, mas pode ajudar a reorganizar o armazenamento
        con.execute(f"ANALYZE {TABLE_NAME};") # Coleta estatísticas para o otimizador de consultas

    except Exception as e:
        print(f"Ocorreu um erro durante a ingestão: {e}")
    finally:
        print("Fechando conexão com o banco de dados.")
        con.close()

if __name__ == "__main__":
    create_duckdb_database()
    print("\nScript de ingestão concluído.")
    print(f"Seu banco de dados DuckDB deve estar em: {DUCKDB_FILE_PATH}")
    print("Lembre-se de adicionar este arquivo .duckdb ao seu .gitignore se estiver usando Git.")