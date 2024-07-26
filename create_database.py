# Library
import psycopg2  # Biblioteca para conectar e interagir com bancos de dados PostgreSQL
import pandas as pd  # Biblioteca para manipulação e análise de dados
import sys  # Biblioteca para manipulação de argumentos da linha de comando
from psycopg2.extensions import connection, cursor  # Importa os tipos connection e cursor de psycopg2
import connect_database

### Script que cria database no banco de dados ###
def create_database(cur: cursor, conn: connection , name_database : str):
    try:
        # Executa o comando SQL para criar a database
        cur.execute(f"CREATE DATABASE {name_database}")
        # Fecha o cursor e a conexão
        cur.close()
        conn.close()

        print(f"Database '{name_database}' criada com sucesso")

    except psycopg2.Error as e:
        cur.close()
        conn.close()
        # Captura e imprime qualquer erro que ocorrer durante a criação da database
        print(f"Ocorreu um erro ao criar a database '{name_database}'\n{e}")
        sys.exit(1)  # Encerra o programa em caso de erro


# COMO USAR 
# # Cria a database utilizando o cursor e a conexão obtidos
# create_database.create_database(cur, conn)