import psycopg2
import pika
import json

from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')

# criando conexão (RABBITMQ)
connection_parameters = pika.connection.URLParameters(config['RABBITMQ']['RABBIT_URI'])
rabbit_queue = config['RABBITMQ']['RABBIT_READ_FROM']
channel = pika.BlockingConnection(connection_parameters).channel()
channel.basic_qos(prefetch_count=1)

def insertQueue(document):
    
    body = json.dumps(document)
    
    channel.basic_publish(
        exchange='',
        routing_key=rabbit_queue,
        body=body
    )
    
    print(body)


def get_products():
    
    pg_sql = """SELECT sku_cd FROM product WHERE catalog_fl = 1 and trim(genero_xml) != 'Moda' order by created_at_dt desc LIMIT 2000000"""
    pg_connect = None
    
    try:
        
        # criando conexão
        pg_connect = psycopg2.connect(config['POSTGRESQL']['URI'])
        # abrindo cursor 
        with pg_connect.cursor() as pg_cursor:
            # batch size 
            pg_cursor.itersize = 1000
            # execute sql 
            pg_cursor.execute(pg_sql)
            
            for result in pg_cursor:
                yield result[0]
        
    except psycopg2.Error as e:
        
        print(f'Query Error: {e}')
        
    finally:
        
        if pg_connect:
            pg_cursor.close()
            pg_connect.close()
            

for index, product in enumerate(get_products()):
    
        settings_name = 'submarino' if index % 2 == 0 else 'shoptime'
        
        document = {
            'url': 'https://www.' + str(settings_name) + '.com.br/produto/' + str(product),
            'settings_name': settings_name
        }
        
        insertQueue(document)
        
        
        