import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import f_oneway, kruskal, shapiro
import streamlit as st

def resumo_estatistico_por_grupo(df, grupo="cluster"):
    return df.groupby(grupo)["quantidade_vendida"].describe()

def teste_normalidade_shapiro(df, coluna="quantidade_vendida"):
    stat, p = shapiro(df[coluna].dropna())
    return stat, p

def teste_anova(df, grupo="cluster"):
    grupos = [grupo_df["quantidade_vendida"].dropna() for nome, grupo_df in df.groupby(grupo)]
    stat, p = f_oneway(*grupos)
    return stat, p

def teste_kruskal(df, grupo="cluster"):
    grupos = [grupo_df["quantidade_vendida"].dropna() for nome, grupo_df in df.groupby(grupo)]
    stat, p = kruskal(*grupos)
    return stat, p

def correlacao(df, metodo="pearson"):
    return df[["idade", "quantidade_vendida"]].corr(method=metodo)

def plot_boxplot(df, grupo="cluster"):
    fig, ax = plt.subplots()
    sns.boxplot(data=df, x=grupo, y="quantidade_vendida", ax=ax)
    st.pyplot(fig)

def plot_histograma(df, coluna="quantidade_vendida"):
    fig, ax = plt.subplots()
    sns.histplot(df[coluna].dropna(), bins=30, kde=True, ax=ax)
    st.pyplot(fig)