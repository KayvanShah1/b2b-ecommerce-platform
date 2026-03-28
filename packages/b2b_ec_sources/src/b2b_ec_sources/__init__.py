import psycopg2
from b2b_ec_utils import settings


def get_connection():
    return psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        user=settings.postgres.user,
        password=settings.postgres.password,
        database=settings.postgres.database,
    )
