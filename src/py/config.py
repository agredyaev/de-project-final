from os import environ

postgres_conn_info = {
    'host': 'rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net',
    'port': 6432,
    'database': 'db1',
    'user': environ.get('POSTGRES_USER'),
    'password': environ.get('POSTGRES_PASS'),
    'sslmode': 'verify-ca',
    'sslrootcert': '/data/CA.pem'
}

vertica_conn_info = {
    'host': 'vertica.tgcloudenv.ru',
    'database': 'dwh',
    'port': 5433,
    'user': environ.get('VERTICA_USER'),
    'password': environ.get('VERTICA_PASSWORD'),
    'autocommit': True,
    'use_prepared_statements': False
}

DIR = '/root/de-project-final/src'
