from functools import cache

import psycopg2
import pycountry
from b2b_ec_utils import settings


def get_connection():
    return psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        user=settings.postgres.user.get_secret_value(),
        password=settings.postgres.password.get_secret_value(),
        database=settings.postgres.database,
    )


@cache
def get_iso_data():
    return [{"code": c.alpha_2, "name": getattr(c, "common_name", c.name)} for c in pycountry.countries]
