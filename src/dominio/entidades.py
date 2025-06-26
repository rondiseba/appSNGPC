class Prescricao:
    def __init__(self, ano, mes, uf, municipio, principio_ativo,
                 descricao_apresentacao, quantidade, unidade, idade, sexo):
        self.ano = int(ano)
        self.mes = int(mes)
        self.uf = uf
        self.municipio = municipio
        self.principio_ativo = principio_ativo
        self.descricao_apresentacao = descricao_apresentacao
        self.quantidade = int(quantidade)
        self.unidade = unidade
        self.idade = int(idade) if idade else None
        self.sexo = sexo