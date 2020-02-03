import os


def postgres_config():
    params = {
        'host': os.environ['POSTGRES_HOST'],
        'port': os.environ['POSTGRES_PORT'],
        'password': os.environ['POSTGRES_PASSWORD'],
        'user': os.environ['POSTGRES_USER']
    }
    return params


def mongodb_config():
    params = {
        'host': os.environ['MONGO_HOST'],
        'port': os.environ['MONGO_PORT']
    }
    uri = 'mongodb://{host}:{port}'.format(**params)
    return uri
