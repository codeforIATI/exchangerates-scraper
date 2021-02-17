# Exchange rates scraper
Scraper to collect historical exchangerates data from OECD and U.S. Federal Reserve. This scraper runs nightly on Github Actions.

Data is stored in `consolidated.csv` file of the [`gh-pages` branch](https://github.com/codeforIATI/exchangerates-scraper/tree/gh-pages).

You can also access the compiled dataset via:
https://codeforiati.org/exchangerates-scraper/consolidated.csv

---

This simple scraper uses the [`exchangerates`](http://github.com/codeforiati/exchangerates) pypi package to generate a rolling set of exchangerates data, sourced from FRED and the OECD. For more information, see the [`exchangerates`](http://github.com/codeforiati/exchangerates) repository.
