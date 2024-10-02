import funcoes #importa o arquivo com as funções
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
ontem = hoje - timedelta(days=30)
ontem = ontem.strftime('%d/%m/%Y')

#monta a url
url = funcoes.api_url
url += f'&data_inicio={ontem}'

#puxa as tabelas da API
dictAPI = {}
print('Lendo API')
dfAPI = funcoes.GetDataApi(url)
print('API lida')
#dfAPI = pd.read_csv('temp/file.csv')
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
    df2 = df2.drop_duplicates()
    dictAPI[key] = df2


#checa se as subtabelas tem todos os ids que vieram no lançamento e salva as que precisa adicionar
linhas = {}
for key, item in dictAPI.items():
    print(key)
    show = []
    df.to_csv(f'temp/{key}.csv')
    ids = dictBancoDados[key]['id'].unique()
    ids = [str(x).strip() for x in ids]
    with open (f'temp/{key}.txt','w') as file:
        file.write(str(ids))
    for i, row in item.iterrows():
        if str(row[f'{key}.id']) not in ids:
            print(f'ID não encontrado {key} - {row[f"{key}.id"]}')
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
for comando in comandos:
    print(comando)
    funcoes.SimpleCommand(comando)
print('Tabelas Auxiliares Upadas')


#trata a tabela principal e tira as duplicatas
seqs = df['id'].unique()
list = []
for i, row in dfAPI.iterrows():
    seq = row['id']
    dfCheck = df[df['id'] == seq]
    if seq not in seqs:
        list.append("True")
    else:
        checker = 0
        for col in df.columns:
            if row[col] != dfCheck[col].iloc[0]:
                checker = 1
        if checker == 1:
            list.append("Change")
        else: 
            print('tudo igual')
            list.append('false')

#filtra as linhas que vão ser upadas e as que vão ser alteradas
dfAPI['show'] = list
dfAPI = dfAPI[dfAPI['show'] == 'True']
dfChange = dfAPI[dfAPI['show'] == 'Change']
dfAPI = dfAPI.drop(columns=['show'])


#upa a tabela principal
funcoes.UparLancamentos(dfAPI)
print('Feito')