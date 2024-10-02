import funcoes #importa o arquivo com as funções
from segredos import passwd, database, user, host, port, token #importa os dados sensíveis que não podem ser upados no github
from datetime import datetime, timedelta #importa a biblioteca de trabalhar com data
import pandas as pd


#lê todas as tabelas que já estão no banco de dados e armazena no dicionario
dictBancoDados = {}
tabelas = funcoes.ListarTabelas()
for tabela in tabelas:
    dictBancoDados[tabela] = funcoes.LerTabelaBancoDados(tabela)
    print(f'{tabela} Lida')

#pega a principal
df = dictBancoDados['lancamentos']

#lista as tabelas e que vão ser atualizadas, armazenando as linhas tb (não faço ideia como vai funcionar)
dictUparBancoDados = {}


'''
aqui vou puxar os dados do sistema
'''
#pega a data de ontem
hoje = datetime.now()
ontem = hoje - timedelta(days=1)
ontem = ontem.strftime('%d/%m/%Y')

#monta a url
url = funcoes.api_url
url += f'&data_inicio={ontem}'

#puxa as tabelas da API
dictAPI = {}
#dfAPI = funcoes.GetDataApi(url)
dfAPI = pd.read_csv('temp/file.csv')
dfAPI = dfAPI[dfAPI['data_inicio'] == ontem]
listaGeral = []

#separa as subtabelas que vieram da api
for key, item in dictBancoDados.items():
    if key == 'lancamentos':
        continue
    list = []
    colunas = dfAPI.columns
    for coluna in colunas:
        if key in coluna:
            list.append(coluna)
            listaGeral.append(coluna)
    df2 = dfAPI[list]
    dictAPI[key] = df2


#checa se as subtabelas tem todos os ids que vieram no lançamento e salva as que precisa adicionar
linhas = {}
for key, item in dictAPI.items():
    show = []
    ids = df[f'{key}.id'].unique()
    for i, row in item.iterrows():
        if str(row[f'{key}.id']) not in ids:
            print(f'ID desconhecido {key} - {row[f"{key}.id"]}')
            show.append('True')
        else:
            show.append('False')
    item['show'] = show
    item = item[item['show'] == 'True']
    item = item.drop_duplicates()
    item = item.drop(columns = ['show'])
    linhas[f'df{key.capitalize()}'] = item

#gera os comandos pras tabelas que precisam
comandos = funcoes.ComandosDict(linhas)
print('Dados Tratados e Comandos Gerados')

#roda os comandos no banco de dados
#for comando in comandos:
    #funcoes.SimpleCommand(comando)
print('Tabelas Auxiliares Upadas')


#trata a tabela principal e tira as duplicatas
seqs = df['sequencial'].unique()
list = []
for i, row in dfAPI.iterrows():
    if row['sequencial'] in seqs:
        list.append("False")
    else:
        list.append("True")
dfAPI['show'] = list
dfAPI = dfAPI[dfAPI['show'] == 'True']
dfAPI = dfAPI.drop(columns=['show'])


#upa a tabela principal
funcoes.UparLancamentos(dfAPI)
print('Feito')