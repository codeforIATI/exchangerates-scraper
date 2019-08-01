from os import environ
from io import StringIO
environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
environ['FRED_API_KEY'] = environ['MORPH_FRED_API_KEY']
import scraperwiki
import csv
import time
from datetime import datetime
import exchangerates.get_rates as gr
import sqlalchemy

key = ['Date', "Currency", "Frequency", "Source"]


class FailedAfterRepeatedAttempts(Exception):
    pass


def save_status(started_at, finished_at=None, success=False):
    scraperwiki.sqlite.save(
        ['started_at'],
        {
            'started_at': started_at,
            'finished_at': finished_at,
            'success': success,
        },
        'status'
    )

scrape_started_at = datetime.now()
save_status(scrape_started_at)


def parse_row(row, attempt=1, speed=0):
    """We were getting OperationalError because of database locks, presumably
    because the database is so large. The database was locking as
    `scraperwiki.sqlite.save()` first attempts to read from the database
    and then saves. `speed` is an attempt to slow things down so that
    hopefully we don't run into these problems. We retry five times, progressively
    increasing the amount of ms we sleep for, by 10ms each time."""
    row["RateFirstSeen"] = datetime.now()
    row["Date"] = row["Date"]
    if speed!=0:
        time.sleep(speed)
    try:
        scraperwiki.sqlite.save(key, row, 'rates')
    except sqlalchemy.exc.OperationalError:
        if attempt == 5:
            raise FailedAfterRepeatedAttempts("""Failed after {} attempts at {}ms
                to import row data {}""".format(attempt, speed, row))
        parse_row(row, attempt=attempt+1, speed=speed+10)
    return speed


def run_scraper():
    gr.update_rates("consolidated.csv")
    the_file = open("consolidated.csv", "r")
    the_csv = csv.DictReader(the_file)
    speed = 0
    print("Downloaded data, loading existing DB data!")
    try:
        db_query = scraperwiki.sql.select("Date, Currency, Frequency, Source from rates;")
        db_data = list(map(lambda r: (r["Date"], r["Currency"], r["Frequency"]), db_query))
    except sqlalchemy.exc.OperationalError:
        db_data = []
    print("Loaded existing data ({} rows), parsing!".format(len(db_data)))

    for i, row in enumerate(the_csv):
        if (row["Date"],
            row["Currency"],
            row["Frequency"]) in db_data:
            continue
        speed = parse_row(row=row, attempt=0, speed=speed)
        if speed == 1000:
            print("Taking too long, stopped at 1000ms!")
            raise
        if i%10000 == 0:
            print("Processing {}th row".format(i))

run_scraper()

scrape_finished_at = datetime.now()
save_status(scrape_started_at, scrape_finished_at, True)