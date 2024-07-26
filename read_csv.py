from psycopg2.extensions import connection, cursor  # Importa os tipos connection e cursor de psycopg2
import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from typing import Optional

def db_csv_func(cur: cursor, conn: connection, url_srv: str, name_table: str, select_columns: str, name_columns: Optional[str] = None):
    try:

        unique_fields = ["id"]  # Lista de campos que devem ser únicos
        # Processar o CSV e obter o DataFrame filtrado
        dfs = process_csv(url_srv, select_columns, name_columns)
        
        if dfs.empty:
            print(f"O DataFrame resultante está vazio. Verifique o arquivo CSV ou a configuração de colunas.")
            return

        # Obter os nomes das colunas do DataFrame
        columns = dfs.columns.tolist()

        # Criar uma string com os nomes das colunas separados por vírgulas
        column_names = ', '.join(columns)

        # Criar uma string de placeholders (%s) para os valores a serem inseridos
        placeholders = ', '.join(['%s'] * len(columns))

        # Cria a string de conflito
        conflict_target = ', '.join(unique_fields)

        print(conflict_target)

        insert_query = f"""
        INSERT INTO {name_table} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_target}) DO NOTHING
        """

        # Preparar os dados para inserção
        data = dfs.values.tolist()
        
        # Executar o comando de inserção em batch
        execute_batch(cur, insert_query, data)

        # print(dfs)
        # print(f"Preenchimento da tabela {name_table} com o arquivo {url_srv} realizado com sucesso")

    except psycopg2.Error as e:
        print(f"Não foi possível ler o arquivo {url_srv} e escrever na tabela {name_table} no banco de dados\n{e}")
        # Em caso de erro, reverter a transação (opcional)
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()

def process_csv(path_srv: str, select_columns: str, name_columns: Optional[str] = None) -> pd.DataFrame:
    try:
        # Ler o arquivo CSV com pandas
        df = pd.read_csv(path_srv, delimiter=';')
        # Substituir NaN por None
        df = df.replace({float('nan'): None})
        
        # Filtrar o DataFrame para as colunas desejadas
        columns_to_import = select_columns.split(',')
        
        # Verificar se todas as colunas a serem importadas estão presentes no DataFrame
        if not all(col in df.columns for col in columns_to_import):
            raise ValueError("Uma ou mais colunas especificadas não estão presentes no CSV.")
        
        # Filtrar o DataFrame para incluir apenas as colunas desejadas
        df_filtered = df[columns_to_import]
        
        # Renomear as colunas se 'name_columns' for fornecido
        if name_columns:
            name_correct = name_columns.split(',')
            
            # Verificar se a lista de novos nomes de colunas tem o mesmo comprimento que o DataFrame filtrado
            if len(name_correct) != len(df_filtered.columns):
                raise ValueError("O número de novos nomes de colunas não corresponde ao número de colunas no DataFrame.")
            
            df_filtered.columns = name_correct

        # Substituir NaN por None
        df_filtered = df_filtered.where(pd.notna(df_filtered), None)

        return df_filtered
    except Exception as e:
        print(f"Erro ao processar o CSV: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    
