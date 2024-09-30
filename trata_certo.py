import requests
import pandas as pd
import psycopg2
from segredos import passwd, database, user, host, port, api_url



#pegar dados da api
def GetDataApi():
    response = requests.get(api_url)
    data = response.json()
    df_final2 = pd.json_normalize(data['abastecimentos'])
    return df_final2

#df = get_data_api

df = pd.read_csv('file.csv')


def UparAuxiliares(df):
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)


    colunas = df.columns
    postos = []
    frentistas = []
    veiculos =[]
    combustiveis = []
    bombas = []
    motoristas = []
    tanques = []
    empresas = []

#listTables = [postos, frentistas, veiculos, combustiveis, bombas, motoristas, tanques, empresas]
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

    dictTable = {}

    for key, item in dictList.items():
        list = []
        for i in colunas:
            if key in i:
                list.append(i)
    #item.append()
    #print(item[0])
        dictTable[f'df{key.capitalize()}'] = df[list].drop_duplicates()

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

#upar_auxiliares(df)


def UparLancamentos(df):
    conn = psycopg2.connect(database = database,
                    user = user,
                    host = host,
                    password = passwd,
                    port = port)


    colunas = df.columns

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

    colunasUsaveis = colunas.to_list()
    colunasPraUsar = []

    for key, item in dictList.items():
        for i in colunas:
            if key in i and '.id' not in i:
                print(i)
                colunasUsaveis.remove(i)
    for i in colunasUsaveis:
        if 'extra' not in i and 'Unnamed' not in i:
            colunasPraUsar.append(i)

    df = df[colunasPraUsar]
    names = df.columns
    for name in names:
        if 'data' in name:
            print(name)
            df[name] = pd.to_datetime(df[name], dayfirst=True, errors='raise')
        else:
            try:
                df[name] = df[name].str.replace('.','')
                df[name] = df[name].str.replace(',','.')
                df[name] = pd.to_numeric(df[name])
            except:
                print(name)


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
            
            

UparLancamentos(df)
 









