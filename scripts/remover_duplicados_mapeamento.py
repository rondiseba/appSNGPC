# Script para remover duplicados do arquivo mapeamento_Controlados.csv
import pandas as pd
from pathlib import Path

# Caminho do arquivo CSV
csv_path = Path(__file__).resolve().parent.parent / 'dados' / 'mapeamento_Controlados.csv'

# Lê o arquivo CSV
mapeamento = pd.read_csv(csv_path, dtype=str)

# Remove duplicados mantendo o primeiro registro de cada principio_ativo
mapeamento_sem_duplicados = mapeamento.drop_duplicates(subset=['principio_ativo'], keep='first')

# Salva o arquivo sem duplicados (sobrescreve o original)
mapeamento_sem_duplicados.to_csv(csv_path, index=False)

print('Duplicados removidos com sucesso!')
