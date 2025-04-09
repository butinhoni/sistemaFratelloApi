from flask import Flask, request, jsonify, make_response
from flask_jwt_extended import(
    JWTManager, create_access_token, create_refresh_token,
    jwt_required, get_jwt_identity, set_access_cookies, set_refresh_cookies,
    unset_jwt_cookies
)
import psycopg2
import segredos
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from dotenv import load_dotenv
import os
import pandas as pd
import funcoes as db

#merged

load_dotenv('.env')

app = Flask(__name__)

app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY') #chave para assinar os tokens
app.config['JWT_TOKEN_LOCATION'] = ['cookies'] #armazenar tokens em cookies
app.config['JWT_COOKIE_CSRF_PROTECT'] = False #proteger contra csrf - ver se arrumo isso depois
app.config['JWT_COOKIE_SECURE'] = False #depois do certbot mudar para TRUE
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = 900 # token de acesso
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = 604800 #7 dias para o token de refresh

jwt = JWTManager(app)

api_token = 'groferequipecoringao2025'

def get_db_connection():
    conn = psycopg2.connect(
        database = segredos.database,
        password = segredos.passwd,
        host = segredos.host,
        port = segredos.port,
        user = segredos.user
    )
    return conn


def check_token():
    token = request.args.get('token') or request.headers.get('Authorization')
    return token == api_token

@app.route('/get_tabela_fratello', methods = ['GET'])
def get_tabela():
    if not check_token:
        return jsonify({'error': 'Token Inválido'})
    
    tabela = request.args.get('tabela')
    if not tabela:
        return jsonify ({'error': 'insira a tabela'})
    
    df = db.LerTabelaBancoDados(tabela)

    df = df.to_json(orient='table')

    return jsonify(df)
if __name__ == 'main':
    app.run(host = '0.0.0.0', port=5000)
