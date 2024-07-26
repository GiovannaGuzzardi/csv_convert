print("### Script que converte excel em alimentação do banco de dados###")
print("### Para converter cria a tabela com as colunas com seus respectivos nomes e linhas devidamente preenchidas e salve como um arquivo csv delimitado por ',' ###")
print("### Necessario instalar pandas ###")

import sys
from connect_database import connect
from read_csv import db_csv_func

# Verifica se um argumento foi passado na linha de comando
if len(sys.argv) < 4 or len(sys.argv) > 5:
    print("Uso: python script.py <url_srv> <name_table> <select_columns,> <name_columns,>")
    sys.exit(1)  # Encerra o programa se nenhum argumento for passado

# Conecta ao banco de dados e obtém o cursor e a conexão
cur, conn = connect()

# Define name_columns com operador ternário
name_columns = sys.argv[4] if len(sys.argv) == 5 else None


db_csv_func(cur=cur ,conn= conn , url_srv=sys.argv[1] , name_table=sys.argv[2] ,  select_columns=sys.argv[3] , name_columns= name_columns )


