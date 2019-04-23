from os import environ
from io import StringIO
environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki
import csv
import time
from datetime import datetime
import exchangerates.get_rates as gr
from sqlite3 import OperationalError

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


def parse_row(row, attempt=1, speed=0)
    """We were getting OperationalError because of database locks, presumably
    because the database is so large. The database was locking as
    `scraperwiki.sqlite.save()` first attempts to read from the database
    and then saves. `speed` is an attempt to slow things down so that
    hopefully we don't run into these problems. We retry five times, progressively
    increasing the amount of ms we sleep for, by 10ms each time."""
    row["RateFirstSeen"] = datetime.now()
    row["Date"] = datetime.strptime(row["Date"], "%Y-%m-%d")
    if speed!=0:
        time.sleep(speed)
    try:
        scraperwiki.sqlite.save(key, row, 'rates')
    except OperationalError:
        if attempt ==5:
            raise FailedAfterRepeatedAttempts("""Failed after {} attempts at {}ms
                to import row data {}""".format(attempt, speed, row))
        parse_row(row, attempt=attempt+1, speed=speed+10)


def run_scraper():
	gr.update_rates("consolidated.csv")
	the_file = open("consolidated.csv", "r")
	the_csv = csv.DictReader(the_file)
	for row in the_csv:
        parse_row(row)

run_scraper()

scrape_finished_at = datetime.now()
save_status(scrape_started_at, scrape_finished_at, True)