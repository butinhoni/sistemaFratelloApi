import funcoes #importa o arquivo com as funções
from datetime import datetime, timedelta #importa a biblioteca de trabalhar com data
import pandas as pd
from modo import tempo_atras




#lê todas as tabelas que já estão no banco de dados e armazena no dicionario
dictBancoDados = {}
tabelas = funcoes.ListarTabelas()
for tabela in tabelas:
    if 'planilhaRDM' in tabela:
        continue
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
ontem = hoje - timedelta(days=tempo_atras)
ontem = ontem.strftime('%d/%m/%Y')



#monta a url
url = funcoes.api_url
url += f'&data_inicio={ontem}'

#puxa as tabelas da API
dictAPI = {}
print('Lendo API')
dfAPI = funcoes.PuxarTudo(url, funcoes.GetDataApi)
print('API lida')
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
    df2.to_csv(f'temp/{key}.csv') #deleta essa merda antes de upar pro git
    dictAPI[key] = df2


#checa se as subtabelas tem todos os ids que vieram no lançamento e salva as que precisa adicionar
linhas = {}
for key, item in dictAPI.items():
    show = []
    ids = dictBancoDados[key]['id'].unique()
    ids = [str(x).strip() for x in ids]
    for i, row in item.iterrows():
        idAPI = row[f'{key}.id']
        if str(idAPI) not in ids:
            print(f'ID não encontrado {key} - {idAPI}')
            show.append('True')
        else:
            show.append('False')
    item['show'] = show
    item = item[item['show'] == 'True']
    item = item.drop_duplicates()
    item = item.drop(columns = ['show'])
    linhas[f'df{key.capitalize()}'] = item

#procura por alterações nas tabelas auxiliares
globalChecker = 1
dadosChange = {}
for key, item in dictAPI.items():
    if key == 'lancamentos':
        continue
    print(f'Procurando atualizações - {key}')
    dfBD = dictBancoDados[key]
    ids = dfBD['id'].unique()
    dfBD = dfBD.set_index('id')
    dfAPI2 = item
    df_mudar = []
    dictDadosLinha = {}
    for numero in ids:
        dadosChangeLinha = []
        dfAPI3 = dfAPI2[dfAPI2[f'{key}.id'] == numero]
        try:
            row = dfAPI3.iloc[-1]
        except:
            continue
        for coluna in dfBD.columns:
            dadoBD = dfBD.loc[numero, coluna]
            dadoAPI = row[f'{key}.{coluna}']
            if dadoAPI != dadoBD:
                dadosChangeLinha.append ({coluna: dadoAPI})
        dictDadosLinha[numero] = dadosChangeLinha
        if numero == '610974386':
            print(dictDadosLinha)
    dadosChange[key] = dictDadosLinha

#atualiza as auxiliares
if globalChecker > 0:
    funcoes.updateTable(dadosChange)

#gera os comandos pras tabelas que precisam de linhas inseridas
comandos = funcoes.ComandosDict(linhas)
print('Dados Tratados e Comandos Gerados')

#roda os comandos no banco de dados pra inserir as linhas 
for comando in comandos:
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
            if 'erro' in col:
                continue
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

for i, row in dfChange.iterrows():
    funcoes.deleteLinhas('lancamentos',row['id'])

#upa a tabela principal
funcoes.UparLancamentos(dfChange)
funcoes.UparLancamentos(dfAPI)
print('Feito')
