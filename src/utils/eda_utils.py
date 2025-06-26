import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit as st

def resumo_estatistico(df, coluna):
    return df[coluna].describe()

def histograma(df, coluna):
    fig, ax = plt.subplots()
    sns.histplot(df[coluna].dropna(), bins=30, kde=True, ax=ax)
    st.pyplot(fig)

def boxplot_categorico(df, categoria, valor):
    fig, ax = plt.subplots()
    sns.boxplot(data=df, x=categoria, y=valor, ax=ax)
    st.pyplot(fig)

def correlacao_entre_colunas(df, col1, col2, metodo="pearson"):
    return df[[col1, col2]].corr(method=metodo)
