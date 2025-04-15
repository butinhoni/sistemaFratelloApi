import pandas as pd
from datetime import datetime
from funcoes import SimpleCommand


def scrapSheet():
    # baixa o excel
    sheet_url = "https://docs.google.com/spreadsheets/d/1-uienEQQZXOX2svsSwB8CI8Ly3Ptrv9AzxDnofmBj0Q/export?format=xlsx"

    # lê as planilhas no arquivo excel
    print('Lendo as plnilhas da pasta')
    pasta = pd.ExcelFile(sheet_url)
    sheets = pasta.book.worksheets
    sheet = sheets[0]
    sheet = sheet.title

    # pega a data e hora
    hoje = datetime.now()


    # função pra iterar as planilhas
    def upfile(pastas, planilha):
        # Lê os dados e encontra a linha do cabeçalho
        print('lendo conteúdo')
        df = pd.read_excel(pastas, planilha)

        linha_colunas = df[df.apply(lambda x: x.astype(str).str.contains('Frota', case=False, na=False).any(), axis=1)].index[0]


        # Define o cabeçalho e remove linhas acima dele
        df.columns = df.iloc[linha_colunas]
        df = df.iloc[linha_colunas + 1:].reset_index(drop=True)


        # Normaliza as colunas e mantém apenas as desejadas
        colunas = ['Nº de Frota','Causa da falha','Observações','Data']
        df = df[colunas]
        df.columns = ['numero', 'causa', 'obs', 'data']
        df['data'] = pd.to_datetime(df['data'], errors = 'coerce', dayfirst=True)
        df['obra'] = planilha
        print(df)
        

        # Remove linhas onde 'Nº de Frota' ou 'Data' estão vazias
        df = df.dropna(thresh = 3)
        df['data'] = df['data'].fillna(pd.to_datetime(datetime.now()))
        
        
        # adiciona a data
        df['dataLeitura'] = hoje
        return(df)
        
    def gerarComandos(df):
        com = []
        for i, row in df.iterrows():
            command = f'INSERT INTO public."planilhaRDM"('
            names = df.columns
            c = 0
            for name in names:
                c += 1
                if c < len(names):
                    command += f'"{name}",'
                else:
                    command += f'"{name}") '
            command += 'VALUES ('
            c = 0
            for name in names:
                if isinstance(row[name], str):
                    valor = row[name].replace("'", '')
                else:
                    valor = row[name]
                c += 1
                if c < len(names):
                    command += f"'{valor}',"
                else:
                    command += f"'{valor}')"
            com.append(command)
        return(com)


    # inicializa a lista de comandos
    comando = []

    # inicializa a lista de tabelas
    tabelas = []



    # gera as tabelas
    for sheet in sheets:
        df = (upfile(sheet_url, sheet.title))
        comandos = (gerarComandos(df))
        for comm in comandos:
            SimpleCommand(comm)
        print(f'{sheet.title} - UPADA')
