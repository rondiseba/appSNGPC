# 📊 Análise de Prescrição de Medicamentos Controlados com IA

Este projeto utiliza dados do SNGPC (2014–2020) para analisar padrões de prescrição no Brasil. Aplica técnicas de Machine Learning para clusterização, séries temporais e detecção de anomalias. Os resultados são exibidos em um dashboard interativo com Streamlit.

---

## 🚀 Funcionalidades

- 📦 ETL e tratamento de dados robusto e eficiente
- 🔍 Análise exploratória (EDA)
- 🧠 Agrupamento por perfis de prescrição (KMeans, DBSCAN)
- 📈 Previsão de consumo com ARIMA
- 🚨 Detecção de anomalias com Isolation Forest
- 📊 Dashboard interativo com navegação por abas
- 🗂️ Remoção automática de duplicidades e padronização de dados
- 🛡️ Validação de dados e tratamento de erros
- 🧪 Testes automatizados para garantir a qualidade do pipeline

---

## 🧱 Estrutura do Projeto

analise_sngpc/  
│  
├── app.py                            # Dashboard principal (Streamlit)  
├── requirements.txt                  # Dependências do projeto  
├── README.md                         # Documentação  
│  
├── config.yaml                       # Configurações do pipeline ETL  
│  
├── dados/                            # Base de dados bruta e processada  
│   ├── Dados_Originais/  
│   │   ├── Dados_BrutosX.csv  
│   │   ├── mesclar_Dados_Brutos.py  
│   │   └── ...  
│   ├── Dados_Brutos_SNGPC.csv  
│   ├── dados_processados.csv         # Dados após ETL  
│   ├── dados_clusterizados.csv       # Dados com clusters  
│   └── anomalias_detectadas.csv      # Dados com anomalias  
│  
├── scripts/  
│   ├── etl.py                        # Executa o processo de ETL  
│   ├── eda.py                        # Análise exploratória  
│   ├── cluster.py                    # Agrupamento KMeans/DBSCAN  
│   ├── previsao.py                   # Execução de previsão ARIMA  
│   └── anomalias.py                  # Execução de detecção de anomalias  
│  
├── tests/  
│   └── test_etl.py                   # Testes automatizados do pipeline  
│  
└── src/  
    ├── __init__.py                   # Torna src um pacote Python  
    │  
    ├── dominio/  
    │   ├── __init__.py  
    │   └── entidades.py              # Entidades do domínio (ex: Prescricao)  
    │  
    ├── infra/  
    │   ├── __init__.py  
    │   └── repositorio_dados.py      # Leitura e limpeza dos dados (ETL)  
    │  
    └── aplicacao/  
        ├── __init__.py  
        ├── analise_eda.py            # Análise exploratória  
        ├── clusterizacao.py          # Algoritmos de agrupamento  
        ├── previsao_temporal.py      # Previsão com ARIMA  
        └── anomalias.py              # Detecção de anomalias  

---

## 💻 Como Executar

1. Clone o repositório:
    ```bash
    git clone https://github.com/seuusuario/seurepositorio.git
    cd seurepositorio
    ```

2. Crie um ambiente virtual e instale as dependências:
    ```bash
    # No Mac
    python -m venv venv
    source venv/bin/activate

    # No Windows
    venv\Scripts\activate

    pip install -r requirements.txt
    ```

3. Configure o arquivo `config.yaml` conforme seu ambiente e caminhos de dados.

4. Execute o ETL dos dados brutos:
    ```bash
    python scripts/etl.py
    ```

5. Execute a clusterização:
    ```bash
    python scripts/cluster.py
    ```

6. Execute a detecção de anomalias:
    ```bash
    python scripts/anomalias.py
    ```

7. Execute a previsão temporal:
    ```bash
    python scripts/previsao.py
    ```

8. Execute o relatório de qualidade - EDA:
    ```bash
    python scripts/eda.py
    ```

9. Execute o dashboard:
    ```bash
    streamlit run app.py
    ```

---

## 🧪 Requisitos

- Python 3.8 ou superior
- Navegador atualizado

---

## 🧪 Testes Automatizados

- Os testes do pipeline ETL estão em `tests/test_etl.py`.
- Execute todos os testes com:
    ```bash
    pytest
    ```

---

## 📄 Licença

Projeto acadêmico desenvolvido por Rondineli Seba. Uso livre para fins educacionais.