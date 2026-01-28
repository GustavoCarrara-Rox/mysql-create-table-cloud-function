# 1. Standard library imports (Python built-ins)
import os
import logging
from datetime import datetime as dt

# 2. Third-party imports
from google.cloud import bigquery
from sqlalchemy import text
import numpy as np

# 3. Local application/library specific imports
from processes.connections import connection_postgresql, connection_mysql

BQ_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
tipo_carga = os.environ.get("TIPO_CARGA")
projeto_localizacao = os.environ.get("PROJETO_LOCALIZACAO")
projeto_regiao = os.environ.get("PROJETO_REGIAO")
dataflow_driver = os.environ.get("DATAFLOW_DRIVER")
dataflow_driver_class = os.environ.get("DATAFLOW_DRIVER_CLASS")
secretmanager_usuario = os.environ.get("SECRET_MANAGER_USER")
secretmanager_senha = os.environ.get("SECRET_MANAGER_PASSWORD")
dataflow_num_workers = os.environ.get("DATAFLOW_NUM_WORKERS")
dataflow_max_workers = os.environ.get("DATAFLOW_MAX_WORKERS")
dataflow_type_machine = os.environ.get("DATAFLOW_TYPE_MACHINE")
manutencao_query = os.environ.get("MANUTENCAO_QUERY")
periodicidade_minutos = os.environ.get("PERIODICIDADE_MINUTOS")
manutencao = os.environ.get("MANUTENCAO")
bigquery_svc = os.environ.get("BIGQUERY_SVC")




bq_client = bigquery.Client()

def create_table_in_bq(project_id, dataset, table_name, schema_fields, partition_column, cluster_fields):

    full_table_id = f"{project_id}.{dataset}.{table_name}"
    table = bigquery.Table(full_table_id, schema=schema_fields)
    
    
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field=partition_column
    )
    
    if cluster_fields:
        clusters = [c.strip() for c in cluster_fields.split(',') if c.strip() and c.lower() != 'null']
        if clusters:
            table.clustering_fields = clusters[:4]

    try:
        bq_client.create_table(table, exists_ok=True)
        logging.info(f"Sucessfully created table: {full_table_id}")
        return True
    except Exception as e:
        logging.error(f"Error creating table {full_table_id}: {e}")
        return False



def insert_monitoring_events(operation: str, table: str, table_id: int, status: str) -> str: 
    session = connection_postgresql()
    today = dt.now()
    event = f"{operation} ON TABLE {table}"

    try:
        # Adicionamos "RETURNING id" (ou o nome da sua chave primária)
        query = text("""
            INSERT INTO public.monitoring_events (codigo_tabela, event, status, event_datetime) 
            VALUES (:codigo_tabela, :event, :status, :today) 
            RETURNING id_monitoring
        """)
        
        monitoring_data = {'codigo_tabela': table_id, 'event': event, 'status': status, 'today': today}
        
        result = session.execute(query, monitoring_data)
        inserted_id = result.scalar() 
        
        session.commit()
        return str(inserted_id)
    except Exception as e:
        logging.error(f"Error logging monitoring event: {e}")
        session.rollback()
        raise
    finally:
        session.close()


def create_table(table_id: int, table_name: str, system: str, table_schema: str) -> bool:
    control_session = connection_postgresql()
    mysql_session = connection_mysql()
    
    try:
        logging.info(f"Starting to create table {table_name}")
        
        ## Checking if table exists 
        sql_query = text("SELECT table_schema, table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME= :tabela_origem AND TABLE_SCHEMA= :schema_origem ")
        information_schema_table = mysql_session.execute(sql_query, {"tabela_origem": table_name, "schema_origem": table_schema})
        rows_information_schema_table = information_schema_table.fetchall()
        
        if rows_information_schema_table != []:
            logging.info(rows_information_schema_table)
            logging.info(f"Table {table_name} exists in the database.")
        else:
            logging.error(f"Table {table_name} doesn't exist in the database.")
        
        for row in rows_information_schema_table:
            array      = np.array(row)
            schema     = array[0]
            source_table_name = array[1]
            source_table_columns = ""
            
            #Save all table's PK's to insert into control table later
            sql_query = text("SELECT COALESCE(GROUP_CONCAT(c.COLUMN_NAME ORDER BY c.ORDINAL_POSITION SEPARATOR ', '), 'null') AS primary_keys FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA AND pk.TABLE_NAME = c.TABLE_NAME AND pk.CONSTRAINT_NAME = c.CONSTRAINT_NAME WHERE pk.CONSTRAINT_TYPE = 'PRIMARY KEY' AND pk.TABLE_SCHEMA = :schema AND pk.TABLE_NAME = :table_name")
            information_schema_column_pk = mysql_session.execute(sql_query, {"schema": schema, "table_name": source_table_name}  )
            rows_information_schema_column_pk = information_schema_column_pk.fetchall()
            for array_pk_columns in rows_information_schema_column_pk:
                array_pk_columns = ', '.join(map(str, array_pk_columns))
                array_pk_columns = array_pk_columns.replace('(', '').replace(')', '')
                source_table_pk = array_pk_columns.replace("'", '')
            
            #Save all the columns and data types to insert into table_mapping later
            sql_query = text(" select column_name, ordinal_position, data_type, coalesce(character_maximum_length,0) as character_maximum_length, coalesce(numeric_precision,0) as numeric_precision, coalesce(numeric_scale,0) as numeric_scale, coalesce(datetime_precision,0) as datetime_precision from INFORMATION_SCHEMA.COLUMNS where table_schema = :schema and table_name = :table_name ")
            information_schema_column_mapping = mysql_session.execute(sql_query, {"schema": schema, "table_name": source_table_name}  )
            rows_information_schema_column_mapping = information_schema_column_mapping.fetchall()
            
            logging.info(f"Inserting columns from table {table_name} into mapping table.")
            
            for array_mapping in rows_information_schema_column_mapping:
                array_mapping = np.array(array_mapping)
                column_name              = array_mapping[0]
                ordinal_position         = array_mapping[1]
                data_type                = array_mapping[2]
                character_maximum_length = array_mapping[3]
                numeric_precision        = array_mapping[4]
                numeric_scale            = array_mapping[5]
                datetime_precision       = array_mapping[6]

                source_table_columns = source_table_columns + column_name + ","
                
                #Inserting into mapping table each column, data type and limits
                try:
                    insert_query = text("INSERT INTO public.table_mapping (codigo_tabela, ordinal_position, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision  ) VALUES (:codigo_tabela, :ordinal_position, :column_name, :data_type, :character_maximum_length, :numeric_precision, :numeric_scale,:datetime_precision  ) ")
                    insert_data = { 'codigo_tabela': table_id, 'ordinal_position': ordinal_position, 'column_name': column_name, 'data_type': data_type, 'character_maximum_length': character_maximum_length, 'numeric_precision': numeric_precision, 'numeric_scale': numeric_scale, 'datetime_precision': datetime_precision }
                    control_session.execute(insert_query, insert_data)
                    control_session.commit()
                    logging.info(f"Successfully inserted {column_name} from table {table_name} into mapping table.")
                    insert_monitoring_events('INSERT', 'table_mapping', table_id, 'OK')
                except Exception as e:
                    logging.error(f"Error inserting data in mapping table for column: {column_name} from table {table_name}. Error: {e}")
                    insert_monitoring_events('INSERT', 'table_mapping', table_id, 'ERROR')
            
            # Creating LAND/RAW/TRUSTED tables
            source_table_columns = source_table_columns[:-1]
            source_table_columns = source_table_columns.lower()
            bigquery_dataset_table = system.lower() + "_" + table_name.lower()
            bigquery_dataset_land = "LAND_" + system.upper()
            bigquery_dataset_raw = "RAW_" + system.upper()
            bigquery_dataset_trusted = "TRUSTED_" + system.upper()
            
            
            # Getting the bq data types mapping    
            sql_query = text(" SELECT tm.column_name, tm.data_type, td.datatype_destino FROM public.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela= :codigo_tabela AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position " )
            mapping_controle = control_session.execute(sql_query, {"codigo_tabela": table_id})
            rows_mapping_controle = mapping_controle.fetchall()
            
            # Definining the sys columns
            base_fields = [
                bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED")
            ]
            schema_land_raw = base_fields.copy()
            schema_trusted = base_fields.copy()
            
            for row in rows_mapping_controle:
                array_columns_bq = np.array(row)
                column_name       = array_columns_bq[0].lower()
                target_data_type  = array_columns_bq[2].lower()
                
                schema_land_raw.append(bigquery.SchemaField(column_name, "STRING"))
                schema_trusted.append(bigquery.SchemaField(column_name, target_data_type))
            
            #LAND
            try:
                logging.info(f"Creating table {table_name} as {bigquery_dataset_table} into {bigquery_dataset_land} dataset")
                create_table_in_bq(BQ_PROJECT_ID, bigquery_dataset_land, bigquery_dataset_table, schema_land_raw, "ingestion_timestamp", source_table_pk)
                insert_monitoring_events('CREATE TABLE', f'{bigquery_dataset_land}.{bigquery_dataset_table}', table_id, 'OK')
            except Exception as e:
                logging.error(f"Error creating table {table_name} as {bigquery_dataset_table} into {bigquery_dataset_land} dataset. Error: {e}")
                insert_monitoring_events('CREATE TABLE', f'{bigquery_dataset_land}.{bigquery_dataset_table}', table_id, 'ERROR')
                raise e
            
            #RAW
            try:
                logging.info(f"Creating table {table_name} as {bigquery_dataset_table} into {bigquery_dataset_raw} dataset")
                create_table_in_bq(BQ_PROJECT_ID, bigquery_dataset_raw, bigquery_dataset_table, schema_land_raw, "ingestion_timestamp", source_table_pk)
                insert_monitoring_events('CREATE TABLE', f'{bigquery_dataset_raw}.{bigquery_dataset_table}', table_id, 'OK')
            except Exception as e:
                logging.error(f"Error creating table {table_name} as {bigquery_dataset_table} into {bigquery_dataset_raw} dataset. Error: {e}")
                insert_monitoring_events('CREATE TABLE', f'{bigquery_dataset_raw}.{bigquery_dataset_table}', table_id, 'ERROR')
                raise e
            
            #TRUSTED
            try:
                logging.info(f"Creating table {table_name} as {bigquery_dataset_table} into {bigquery_dataset_trusted} dataset")
                create_table_in_bq(BQ_PROJECT_ID, bigquery_dataset_trusted, bigquery_dataset_table, schema_trusted, "ingestion_timestamp", source_table_pk)
                insert_monitoring_events('CREATE TABLE', f'{bigquery_dataset_trusted}.{bigquery_dataset_table}', table_id, 'OK')
            except Exception as e:
                logging.error(f"Error creating table {table_name} as {bigquery_dataset_table} into {bigquery_dataset_trusted} dataset. Error: {e}")
                insert_monitoring_events('CREATE TABLE', f'{bigquery_dataset_raw}.{bigquery_dataset_table}', table_id, 'ERROR')
                raise e
            
            bucket_land  = BQ_PROJECT_ID.lower() + "/land/" + system.lower() + "/" + bigquery_dataset_table
            bucket_raw = BQ_PROJECT_ID.lower() + "/raw/" + system.lower() + "/" + bigquery_dataset_table
            
            # Updating control table with the ingestion metadata
            try:
                logging.info(f"Updating table {table_name} with table id {table_id}.")
                update_query = text("UPDATE public.controle SET dt_carga_ini=now(), dt_carga_fim=now(), bigquery_dataset_tabela=:bigquery_dataset_tabela, bucket_land=:bucket_land, bucket_raw=:bucket_raw, bigquery_dataset_land=:bigquery_dataset_land, bigquery_dataset_raw=:bigquery_dataset_raw, bigquery_dataset_trusted=:bigquery_dataset_trusted , bigquery_create_table='N', tabela_colunas_origem=:tabela_colunas_origem, tabela_origem_pk=:tabela_origem_pk, bigquery_partition='ingestion_timestamp', bigquery_clustered=:tabela_origem_pk, tipo_carga=:tipo_carga, projeto_localizacao=:projeto_localizacao, projeto_regiao=:projeto_regiao, dataflow_driver=:dataflow_driver, dataflow_driver_class=:dataflow_driver_class, secretmanager_usuario=:secretmanager_usuario, secretmanager_senha=:secretmanager_senha, dataflow_num_workers=:dataflow_num_workers, dataflow_max_workers=:dataflow_max_workers, dataflow_type_machine=:dataflow_type_machine, manutencao_query=:manutencao_query, periodicidade_minutos=:periodicidade_minutos, manutencao=:manutencao, bigquery_svc=:bigquery_svc, projeto=:projeto WHERE codigo=:codigo_tabela ")
                update_data = {'codigo_tabela': table_id, 'bigquery_dataset_tabela': bigquery_dataset_table, 'bucket_land': bucket_land, 'bucket_raw': bucket_raw, 'bigquery_dataset_land': bigquery_dataset_land, 'bigquery_dataset_raw': bigquery_dataset_raw, 'bigquery_dataset_trusted': bigquery_dataset_trusted, 'tabela_colunas_origem': source_table_columns ,'tabela_origem_pk': source_table_pk, 'tipo_carga': tipo_carga, 'projeto_localizacao':projeto_localizacao, 'projeto_regiao':projeto_regiao, 'dataflow_driver':dataflow_driver, 'dataflow_driver_class':dataflow_driver_class, 'secretmanager_usuario':secretmanager_usuario, 'secretmanager_senha':secretmanager_senha, 'dataflow_num_workers':dataflow_num_workers, 'dataflow_max_workers':dataflow_max_workers, 'dataflow_type_machine':dataflow_type_machine, 'manutencao_query':manutencao_query, 'periodicidade_minutos':periodicidade_minutos, 'manutencao':manutencao, 'bigquery_svc':bigquery_svc, 'projeto':BQ_PROJECT_ID }
                control_session.execute(update_query, update_data)
                control_session.commit()
                logging.info(f"Successfulyy updated table {table_name} with table id {table_id}.")

                insert_monitoring_events('UPDATE', 'controle', table_id, 'OK')
            except Exception as e:
                logging.error(f"Error updating table {table_name} with table id {table_id}. Error: {e}")
                insert_monitoring_events('UPDATE', 'controle', table_id, 'ERROR')
                raise e     
        
        return {table_name: True}
    except Exception as e:
        logging.exception(f"Error creating table {table_name}: {e}")
        return {table_name: None}
    finally:
        control_session.close()
        mysql_session.close()