from functools import cache

import psycopg2
import pycountry
from b2b_ec_utils import settings


def get_connection():
    connect_kwargs = dict(
        host=settings.postgres.host,
        port=settings.postgres.port,
        user=settings.postgres.user.get_secret_value(),
        password=settings.postgres.password.get_secret_value(),
        database=settings.postgres.database,
        sslmode=settings.postgres.sslmode,
    )
    if settings.postgres.sslrootcert:
        connect_kwargs["sslrootcert"] = str(settings.postgres.sslrootcert)

    return psycopg2.connect(**connect_kwargs)


@cache
def get_iso_data():
    return [{"code": c.alpha_2, "name": getattr(c, "common_name", c.name)} for c in pycountry.countries]
