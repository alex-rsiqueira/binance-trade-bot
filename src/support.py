import os
import json 
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
# from google.cloud import secretmanager
from google.cloud.bigquery import SchemaField

PROJECT_ID = 'cripto-trade-2024'#os.environ.get("PROJECT_ID")
DATASET_ID = 'raw'

def log_error(error, desc_e = None, project_id = PROJECT_ID, dataset_id = DATASET_ID, trade_symbol = None, operation_type = None):

    error_code = error.args[0]
    message = str(error)
    print(f"[ERROR] {desc_e} - {message}")

    log_table = pd.DataFrame(columns=['log_date', 'trade_symbol', 'operation_type', 'log_type', 'error_code', 'message', 'description'])
    log_date = datetime.now()
    new_line = [{'log_date': log_date, 'trade_symbol': trade_symbol, 'operation_type': operation_type, 'log_type': 'Error', 'error_code': error_code, 'message': message, 'description': desc_e}]
    log_table = pd.concat([log_table, pd.DataFrame(new_line, index=[0])], ignore_index=True)
    
     # Configurar o nome completo da tabela
    log_table_name = 'tb_operation_log'

    insert_db(log_table,log_table_name,dataset_id,project_id)

def identify_error(table_id,e,dataset_id,project_id):
    
    print('Registrando erro: ',e)
    
    if isinstance(e, json.JSONDecodeError):
        desc_e= 'Erro de decodificação JSON'
        log_error(e,desc_e,project_id,dataset_id,table_id)
    elif isinstance(e, requests.HTTPError):
        desc_e= 'Erro de requisição HTTP'
        log_error(e,desc_e,project_id,dataset_id,table_id)
    # elif isinstance(e, pyodbc.Error):
    #     desc_e= 'Erro de banco'
    #     # status_code = response.status_code
    #     log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    # elif isinstance(e, requests.RequestException):
        # pag=-1
        # desc_e= 'Erro de excessão da classe request'
        # # status_code = e.response.status_code if e.response is not None else 'Desconhecido'
        # log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    else:
        desc_e= 'Erro desconhecido'
        log_error(e,desc_e,project_id,dataset_id,table_id)

def generate_bigquery_schema(df: pd.DataFrame) -> list[SchemaField]:
    TYPE_MAPPING = {
        "i": "INTEGER",
        "u": "NUMERIC",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }

    column_mapping = {'user': 'STRING'
                    ,'close': 'FLOAT'
                    ,'var': 'FLOAT'
                    ,'positivo': 'FLOAT'
                    ,'negativo': 'FLOAT'
                    ,'media_positivos': 'FLOAT'
                    ,'media_negativo' : 'FLOAT'
                    ,'position': 'STRING'
                    ,'quantity': 'FLOAT'
                    ,'symbol': 'STRING'
                    ,'coin_balance': 'FLOAT'
                    ,'coin_wltcoin_balance': 'FLOAT'
                    ,'profit_value': 'FLOAT'
                    ,'in_position_before': 'BOOLEAN'
                    ,'in_position_after': 'BOOLEAN'
                    ,'hold': 'BOOLEAN'
                    ,'venda': 'BOOLEAN'
                    ,'compra': 'BOOLEAN'
                    ,'wltcoin_balance_before': 'FLOAT'
                    ,'wltcoin_balance_after': 'FLOAT'
                    ,'dt_insert':'TIMESTAMP'}

    schema = []
    for column, dtype in df.dtypes.items():

        val = df[column].iloc[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"
        if mode == "REPEATED" and len(df[df[column].apply(len) > 0]) > 0:
            val = df[df[column].apply(len) > 0][column].iloc[0]

        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(next(iter(val), None), dict)):
            fields = generate_bigquery_schema(pd.json_normalize(val))
        else:
            fields = ()

        type = "RECORD" if fields else TYPE_MAPPING.get(dtype.kind)
        type = column_mapping.get(column) if column in column_mapping.keys() else type
        schema.append(
            SchemaField(
                name=column,
                field_type=type,
                mode=mode,
                fields=fields,
            )
        )
    return schema

def insert_db(df,table_id,dataset_id=DATASET_ID,project_id=PROJECT_ID):
     # Configurar o cliente do BigQuery
    try:
        bq_client = bigquery.Client(project=project_id)
        
        # Configurar o nome completo da tabela
        table_ref = f'{project_id}.{dataset_id}.{table_id}'

        bq_table = bigquery.Table(table_ref, schema=generate_bigquery_schema(df))
        try:
            # Check if table exists
            bq_client.get_table(bq_table).schema
        except:
            # Create table if doesn't exist
            bq_client.create_table(bq_table)

        # Inserir o DataFrame na tabela (cria a tabela se não existir, trunca se existir)
        # pd.io.gbq.to_gbq(df, destination_table=table_ref, if_exists='append', project_id=project_id)
        result = bq_client.insert_rows(table_ref, df.replace(np.nan, None).to_dict(orient='records'),selected_fields=generate_bigquery_schema(df))

        if result == []:
            print(f"Tabela populada com sucesso: {table_id}")
        else:
            raise(result)
    except Exception as e:
        identify_error(table_id,e,dataset_id,project_id)