import seaborn as sns
import matplotlib.pyplot as plt

def gerar_relatorio_qualidade(df):
    print("Resumo estatístico:")
    print(df.describe(include='all'))
    print("\nValores nulos por coluna:")
    print(df.isnull().sum())
    sns.histplot(df['idade'].dropna(), bins=20, kde=True)
    plt.title("Distribuição de Idade")
    plt.show()