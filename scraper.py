from os import environ
from io import StringIO
environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki
import csv
from datetime import datetime
import exchangerates.get_rates as gr

key = ['Date', "Currency", "Frequency", "Source"]


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

scrape_started_at = datetime.now().isoformat()
save_status(scrape_started_at)

def run_scraper():
	gr.update_rates("consolidated.csv")
	the_file = open("consolidated.csv", "r")
	the_csv = csv.DictReader(the_file)
	for row in the_csv:
		row["RateFirstSeen"] = datetime.now().isoformat()
		row["Date"] = datetime.strptime(row["Date"], "%Y-%m-%d")
		scraperwiki.sqlite.save(key, row, 'rates')

run_scraper()

scrape_finished_at = datetime.now().isoformat()
save_status(scrape_started_at, scrape_finished_at, True)