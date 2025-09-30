## JobsDB HK Scraper (AI/ML/Data Engineer)

This Python scraper collects current Hong Kong job openings from JobsDB for target keywords (AI, Machine Learning, AI Research, Data Engineer). It extracts:

- Job title
- Company name
- Company profile URL (if present)
- Location
- Posted time
- Job URL
- Company industry (best-effort from company page)
- Company founded date (best-effort from company page)

Outputs are saved as CSV and Excel in the project directory.

### 1) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Install Playwright browser binaries (Chromium only is sufficient)
python -m playwright install chromium
```

### 2) Run

```bash
python jobsdb_scraper.py
```

This will create `jobsdb_hk_jobs.csv` and `jobsdb_hk_jobs.xlsx`.

### 3) Adjust keywords or pages

Edit `TARGET_KEYWORDS` in `jobsdb_scraper.py`. You can also change `max_pages` inside `scrape_keyword(kw, max_pages=3)`.

### Notes

- Company industry/founded are parsed heuristically from company profile pages and may be missing or approximate.
- Be considerate: avoid very high page limits or rapid re-runs.
- This is for research use; review JobsDB terms of use and robots.txt before large-scale scraping.


