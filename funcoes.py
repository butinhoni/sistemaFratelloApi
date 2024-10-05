import requests #importa a biblioteca que puxa os dados da API
import pandas as pd #importa o pandas pra tratar os dados
import psycopg2 #importa a biblioteca que faz a ligação com o PostgreSQL
from segredos import passwd, database, user, host, port, token #importa os dados sensíveis que não podem ser upados no github
import time #biblioteca que usa pra poder esperar o danado do minuto
from datetime import datetime


#cria a api do request
api_url = f'https://www.ctasmart.com.br:8443/SvWebSincronizaAbastecimentos?token={token}&formato=json'

def SimpleCommand(comand):
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)
    cur = conn.cursor()
    cur.execute(comand)
    cur.close()
    conn.commit()
    conn.close()


def GetDataApi(api_url):
    '''
    função que devolve os dados pegos da api
    '''
    #puxa os dados
    response = requests.get(api_url)
    data = response.json()

    #trata os dados da api
    df_final2 = pd.json_normalize(data['abastecimentos'])
    df_final2.to_csv('temp/file.csv')
    return df_final2


def UparAuxiliares(df):
    '''
    empacotando tudo em função pra caso precise usar em outros lugares, essa é a que gera e upa todas as tabelas exceto a principal
    talvez seja o caso de futuramente separar em duas
    '''
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)


    #leio todas as colunas da tabela cheia e normalizada
    colunas = df.columns

    #crio uma lista pra cada uma das tabelas que quero criar, pra armazenas as colunas de cada uma

    #olhando agora muito provavelmente essas listas não precisam existir e o dicionario poderia ser uma lista
    #vou deixar esse comentário pra ajeitar quando tiver tempo
    postos = []
    frentistas = []
    veiculos =[]
    combustiveis = []
    bombas = []
    motoristas = []
    tanques = []
    empresas = []

    #crio um dicionario com palavra-chave pra cada lista, pra poder fazer o loop e separar as colunas que quero pra cada uma
    dictList = {
    'posto': postos,
    'frentista': frentistas,
    'veiculo': veiculos,
    'combustivel': combustiveis,
    'bomba':bombas,
    'motorista':motoristas,
    'tanque':tanques,
    'empresa':empresas
    }

    #crio um dicionario vazio pra armazenar palavra-chave pra cada dataframe que for criado
    dictTable = {}

    #cria um dataframe pra cada item no dicionario dictList
    for key, item in dictList.items():
        #cria uma lista pra cada item do dictList, pega as colunas que contenham a palavra-chave no nome
        list = []
        for i in colunas:
            if key in i:
                list.append(i)
        #cria um dataframe pegando só as colunas listadas e remove as linhas duplicatas.
        dictTable[f'df{key.capitalize()}'] = df[list].drop_duplicates()


    #nesse ponto aqui as tabelas já estão prontas, o que falta é subir pro postgreSQL só

    '''
    essa parte é um tanto complicada pra explicar e precisa entender um pouquinho de SQL, mas em resumo ela
    cria comandos pra upar todas as tabelas de uma vez só no banco de dados
    '''
    for key, item in dictTable.items():
        key = key.replace('df','').lower()
        id = f'{key}.id'
        print(id)
        item = item[item[id] != 0]
        item = item.dropna(subset=[id])
        for i, r in item.iterrows():
            command = f'INSERT INTO public.{key}('
            names = item.columns
            namesPython = []
            c = 0
            for name in names:
                c += 1
                name = name.replace(f'{key}.','')
                if c < len(names):
                    command += f'"{name}",'
                else:
                    command += f'"{name}") '
                namesPython.append(f'{key}.{name}')
            command += 'VALUES ('
            c = 0
            for name in namesPython:
                c += 1
                if c < len(namesPython):
                    command += f"'{r[name]}',"
                else:
                    command += f"'{r[name]}')"
            print(command)
            cur = conn.cursor()
            cur.execute(command)
            cur.close()
            print(f'{command} - FEITO')
    conn.commit()
    conn.close()


def UparLancamentos(df):
    '''
    essa cria e upa a tabela de lançamentos, de um jeito bem parecido com a outra função
    '''
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)


    colunas = df.columns

    #olhando agora muito provavelmente essas listas não precisam existir e o dicionario poderia ser uma lista
    #vou deixar esse comentário pra ajeitar quando tiver tempo
    postos = []
    frentistas = []
    veiculos =[]
    combustiveis = []
    bombas = []
    motoristas = []
    tanques = []
    empresas = []

    dictList = {
    'posto': postos,
    'frentista': frentistas,
    'veiculo': veiculos,
    'combustivel': combustiveis,
    'bomba':bombas,
    'motorista':motoristas,
    'tanque':tanques,
    'empresa':empresas
    }

    #olhando de novo tem lista demais nesse código, dava pra ter usado duas ao invés de três
    #cria duas listas, uma com as colunas e outra pra armazenar as colunas filtradas.
    colunasUsaveis = colunas.to_list()
    colunasPraUsar = []

    for key, item in dictList.items():
        #remove as colunas que tem a palavra-chave das colunas usaveis exceto a que tem o id
        for i in colunas:
            if key in i and '.id' not in i:
                colunasUsaveis.remove(i)
    #remove as colunas que são lixo
    for i in colunasUsaveis:
        if 'extra' not in i and 'Unnamed' not in i:
            colunasPraUsar.append(i)

    #filtra o dataframe pra usar só as colunas selecionadas
    df = df[colunasPraUsar]
    names = df.columns

    #formata as colnas direitinho pra upar pro banco de dados
    for name in names:
        if name == 'combustivel.id':
            df[name] = df[name].astype(str).replace('2','2.0')
            df[name] = df[name].astype(str).replace('3','3.0')   
        if 'id' in name:
            continue
        if 'data' in name:
            df[name] = pd.to_datetime(df[name], dayfirst=True, errors='raise')
        else:
            try:
                df[name] = df[name].str.replace('.','')
                df[name] = df[name].str.replace(',','.')
                df[name] = pd.to_numeric(df[name])
            except:
                pass


    #cria um comando pra cada linha e upa elas no banco de dados
    for i, row in df.iterrows():
        command = 'INSERT INTO public.lancamentos('
        c = 1
        for name in names:
            command += f'"{name}"'
            if c < len(names):
                command += ','
            else:
                command += ') '
            c+=1
        command += 'VALUES ('
        c = 1
        for name in names:
            command += f"'{row[name]}'"
            if c < len(names):
                command += ', '
            else:
                command += ') '
            c+=1
        print(command)
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
        print('feito')
    conn.commit()
    conn.close()
        

def PuxarTudo(api_url, GetDataApi):
    """
    função pra fazer requests na API até que pegue todos os dados
    """

    #lê faz o primeiro request, pegando os arquivos mais antigos
    print('primeira leitura da API')
    df = GetDataApi(api_url)

    #ajusta as coisas e pega a data do ultimo lançamento que veio no sistema
    df['data_inicio'] = pd.to_datetime(df['data_inicio'], dayfirst=True)
    last = df['data_inicio'].max()
    last_f = last.strftime('%d/%m/%Y')
    hoje = datetime.today()
    hoje = pd.to_datetime(hoje)
    lastA = 0


    #lista vazia pra armazenar as tabelas e um contador pra ir salvando em pdf também
    lista = []
    count = 0
    #enquanto a data do ultimo lançamento for anterior a hoje
    while last <= hoje:
        #checa se teve atualização na lista
        if last == lastA:
            break
        lastA = last
        #espera um minuto (limitação da api)
        print('Esperando um minuto')
        count += 1
        time.sleep(60)

        #gera a url do request tendo a data de incio = a ultima data que veio
        url = f'https://www.ctasmart.com.br:8443/SvWebSincronizaAbastecimentos?token={token}&formato=json&data_inicio={last_f}'

        #faz o request em si
        print('começando busca na API')
        try:
            df = GetDataApi(url)
        except:
            print('erro pegando dados')
            print(url)
            break
        
        #junta o resultado na lista e salva em csv também
        lista.append(df)
        df.to_csv(f'temp/{count}.csv')

        #pega de novo a ultima data que tem
        try:
            df['data_inicio'] = pd.to_datetime(df['data_inicio'], dayfirst = True)
            last = df['data_inicio'].max()
            last_f = last.strftime('%d/%m/%Y')
            print(f'Ultima data: {last_f}')
        except:
            print('erro tratando a tabela')
            print(df)
            lista.append(df)
            df.to_csv(f'temp/{count}.csv')
            break
    
    #junta todas as tabelas e retorna uma só
    df_up = pd.concat(lista)
    df_up = df_up.drop_duplicates()
    return df_up
    


def LerTabelaBancoDados(name):
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)  
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM public.{name}')
    colunas = []
    for item in cur.description:
        colunas.append(item[0])
    dados = cur.fetchall()
    df = pd.DataFrame(dados, columns = colunas)
    cur.close()
    conn.close()
    return(df)

def ListarTabelas():
    conn = psycopg2.connect(database = database,
                user = user,
                host = host,
                password = passwd,
                port = port)  
    cur = conn.cursor()
    cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    list = cur.fetchall()
    tabelas = []
    for i in list:
        tabelas.append(i[0])
    cur.close()
    conn.close()
    return(tabelas)


def ComandosDict(dictTable):
    listaComandos = []
    for key, item in dictTable.items():
        key = key.replace('df','').lower()
        id = f'{key}.id'
        item = item[item[id] != 0]
        item = item.dropna(subset=[id])
        for i, r in item.iterrows():
            command = f'INSERT INTO public.{key}('
            names = item.columns
            namesPython = []
            c = 0
            for name in names:
                c += 1
                name = name.replace(f'{key}.','')
                if c < len(names):
                    command += f'"{name}",'
                else:
                    command += f'"{name}") '
                namesPython.append(f'{key}.{name}')
            command += 'VALUES ('
            c = 0
            for name in namesPython:
                c += 1
                if c < len(namesPython):
                    command += f"'{r[name]}',"
                else:
                    command += f"'{r[name]}')"
            listaComandos.append(command)
            return(listaComandos)

def deleteLinhas(tabela, ids):
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)  
    cur = conn.cursor()
    for id in ids:
        cur.execute(f'''
                    DELETE FROM public.{tabela}
                    WHERE "id" = '{id}' 
                    ''')
        print(f'{id} deletado')
    cur.close()
    conn.commit()
    conn.close()