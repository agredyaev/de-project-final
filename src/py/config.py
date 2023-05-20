# Define the connection information for the databases
from os import environ

postgres_conn_info = {
    'host': 'rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net',
    'port': 6432,
    'database': 'db1',
    'user': environ.get('POSTGRES_USER'),
    'password': environ.get('POSTGRES_PASS'),
    'sslmode': 'verify-ca',
    'sslrootcert': '/path/to/certificate.crt'
}

vertica_conn_info = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': environ.get('VERTICA_USER'),
    'password': environ.get('VERTICA_PASSWORD')
}


