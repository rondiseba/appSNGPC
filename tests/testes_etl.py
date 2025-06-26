import pytest
import duckdb
from pathlib import Path
from scripts.etl import executar_pipeline_etl_sql
from src.utils.database_utils import get_db_connection_for_etl

def test_etl_pipeline_runs(tmp_path):
    # Setup: cria um banco temporário e pasta de dados
    db_path = tmp_path / "test.duckdb"
    dados_path = tmp_path / "dados"
    dados_path.mkdir()
    # (Opcional: copie um CSV de exemplo para dados_path)

    # Executa o pipeline
    conexao = duckdb.connect(str(db_path))
    try:
        executar_pipeline_etl_sql(conexao, dados_path)
        # Verifica se a tabela final foi criada
        tabelas = conexao.execute("SHOW TABLES").fetchall()
        assert any("prescricoes" in t for t in [x[0] for x in tabelas])
    finally:
        conexao.close()