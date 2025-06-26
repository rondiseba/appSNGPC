# src/utils/stats_utils.py
import pandas as pd
from scipy.stats import shapiro, f_oneway

def realizar_teste_shapiro(series_data, nome_coluna_para_msg, max_samples_for_shapiro=5000):
    """
    Realiza o teste de Shapiro-Wilk em uma pandas Series.
    Se a amostra for > max_samples_for_shapiro, testa uma subamostra aleatória.
    Retorna: stat, p_valor, erro_msg, mensagem_subamostragem
    """
    dados_validos = series_data.dropna()
    n_samples = len(dados_validos)
    if n_samples < 3:
        return None, None, f"Não há dados suficientes (mínimo 3) em '{nome_coluna_para_msg}'. Encontrados: {n_samples}.", ""
    
    dados_para_teste = dados_validos
    mensagem_subamostragem = ""

    if n_samples > max_samples_for_shapiro:
        mensagem_subamostragem = (
            f"A amostra original para '{nome_coluna_para_msg}' com {n_samples:,} pontos excede o limite. "
            f"O teste foi feito em uma subamostra de {max_samples_for_shapiro:,}."
        )
        try:
            dados_para_teste = dados_validos.sample(n=max_samples_for_shapiro, random_state=42)
        except ValueError: 
            dados_para_teste = dados_validos 
            mensagem_subamostragem = "" 
            
    try:
        stat, p_valor = shapiro(dados_para_teste)
        return stat, p_valor, None, mensagem_subamostragem 
    except Exception as e:
        return None, None, f"Erro ao executar Shapiro-Wilk: {e}", mensagem_subamostragem

def realizar_teste_anova(df_test, coluna_valor, coluna_grupo):
    """Realiza o teste ANOVA em um DataFrame, com subamostragem por grupo."""
    if coluna_valor not in df_test.columns or coluna_grupo not in df_test.columns:
        return None, None, "Coluna não encontrada."
    if not pd.api.types.is_numeric_dtype(df_test[coluna_valor]):
        return None, None, f"'{coluna_valor}' não é numérica."
    if df_test[coluna_grupo].dropna().empty:
        return None, None, f"'{coluna_grupo}' está vazia."

    grupos_distintos = df_test[coluna_grupo].dropna().unique()
    if len(grupos_distintos) < 2:
        return None, None, "São necessários pelo menos 2 grupos."

    dados_por_grupo = [df_test[df_test[coluna_grupo] == g][coluna_valor].dropna() for g in grupos_distintos]
    dados_por_grupo = [g for g in dados_por_grupo if len(g) >= 1]

    if len(dados_por_grupo) < 2:
        return None, None, "Não há grupos suficientes com dados válidos."
        
    try:
        n_subsample = 5000
        subamostras = [g.sample(n=n_subsample, random_state=42) if len(g) > n_subsample else g for g in dados_por_grupo]
        stat, p_valor = f_oneway(*subamostras)
        return stat, p_valor, None
    except Exception as e:
        return None, None, f"Erro ao executar ANOVA: {e}"