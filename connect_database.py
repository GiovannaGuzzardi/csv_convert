# Library
import psycopg2  # Biblioteca para conectar e interagir com bancos de dados PostgreSQL
import pandas as pd  # Biblioteca para manipulação e análise de dados
import sys  # Biblioteca para manipulação de argumentos da linha de comando
from psycopg2.extensions import connection, cursor  # Importa os tipos connection e cursor de psycopg2

def connect():
    # Parâmetros para conexão no banco de dados
    db_params = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'nova_senha'
    }

    try:
        # Criação da conexão com o banco de dados PostgreSQL
        conn = psycopg2.connect(
            host=db_params['host'],
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password']
        )

        # Criação do objeto cursor que permite executar comandos SQL
        cur = conn.cursor()

        # Configura a conexão para confirmar transações automaticamente após cada comando SQL
        conn.set_session(autocommit=True)

        # Retorna o cursor e a conexão
        return cur, conn
    
    except psycopg2.Error as e:
        cur.close()
        conn.close()

        # Captura e imprime qualquer erro que ocorrer durante a conexão com o banco de dados
        print(f"Ocorreu um erro ao criar a conexão com a database\n{e}")
        sys.exit(1)  # Encerra o programa em caso de erro

# como usar
# # Conecta ao banco de dados e obtém o cursor e a conexão
# cur, conn = connect_database.connect()