"""Pull the customer dimension from the Waseet API into the warehouse.

    python ingest_customers.py

Run this once before the first scan load, because `pipeline.py` checks
customer_id against this table.

The API is at ../api/waseet_api.py. Start it in-process:

    import sys
    sys.path.insert(0, "../api")
    from waseet_api import start_api
    BASE_URL = start_api()

Three things about it will bite a client that assumes the easy case, and all
three were in Lecture 7:

  * the key goes in the X-API-Key header, not the query string
  * the answer is an envelope; the records are under "customers"
  * the first attempt at every third page comes back 429

Two hundred customers arrive over eight pages. If your pull finishes with fewer
than two hundred and does not complain, that is the bug this API exists to find.
"""

import os

import pandas as pd
import requests
from sqlalchemy import create_engine

API_KEY = "waseet-demo-key"
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")

engine = create_engine(DB_URL)

import time
import requests
def get_with_retry(url, headers, params=None, attempts=4):
    wait = 1
    for attempt in range(attempts):
        reply = requests.get(url, params=params, headers=headers, timeout=5)
        
        if reply.status_code != 429 and not (500 <= reply.status_code < 600):
            return reply
            
        print(f"  Transient error {reply.status_code} on attempt {attempt + 1} - waiting {wait} seconds")
        time.sleep(wait)
        wait = wait * 2
        
    return reply
    raise NotImplementedError


def fetch_all_customers(base_url):
    """TODO. Page until has_more is False. Return a list of dicts.

    Drive the loop on what the API tells you, not on a page count you worked out
    yourself - the second one is right until the day the data grows.
    """
    raise NotImplementedError


def load_customers(records):
    """TODO. Land them in the customers table.

    Idempotent: running this twice must not double the dimension, and must not
    fail either. The table has a primary key, which rules out a plain append.
    """
    raise NotImplementedError


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "../api")
    from waseet_api import start_api

    base_url = start_api()
    print("API on", base_url)

    records = fetch_all_customers(base_url)
    print("fetched", len(records), "customers")

    load_customers(records)
    print("loaded")
