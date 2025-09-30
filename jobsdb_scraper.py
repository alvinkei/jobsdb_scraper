import asyncio
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from urllib.parse import quote_plus

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from playwright.async_api import async_playwright, Page


BASE_URL = "https://hk.jobsdb.com"
# Use the search URL directly to avoid fragile UI interactions
# Example: https://hk.jobsdb.com/hk/search-jobs/Data%20Engineer?page=2
def build_search_url(keyword: str, page: int = 1) -> str:
    encoded = quote_plus(keyword)
    base = f"{BASE_URL}/hk/search-jobs/{encoded}"
    if page and page > 1:
        return f"{base}?page={page}"
    return base


TARGET_KEYWORDS = [
    "Machine Learning",
    "AI",
    "AI Research",
    "Data Engineer",
    "Data Scientist",
    "Data Analyst",
    "Data Architecture",
    "Data Modeling",
    "Data Engineering",
    "Data Science"
]


@dataclass
class JobRecord:
    keyword: str
    job_title: str
    company_name: str
    company_profile_url: Optional[str]
    location: Optional[str]
    posted_time: Optional[str]
    job_url: Optional[str]
    industry: Optional[str]
    founded: Optional[str]


async def ensure_page_ready(page: Page) -> None:
    await page.wait_for_load_state("domcontentloaded")
    # Accept cookies if banner appears
    try:
        # Try common cookie button selectors/texts
        await page.get_by_role("button", name=lambda n: n and ("accept" in n.lower() or "agree" in n.lower())).click(timeout=2000)
    except Exception:
        pass


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def goto_search_results(page: Page, keyword: str, page_num: int = 1) -> None:
    url = build_search_url(keyword, page_num)
    await page.goto(url, timeout=90000)
    await ensure_page_ready(page)
    # Wait for job list container/cards
    try:
        await page.wait_for_selector("[data-automation='job-card-list'], [data-automation='job-card']", timeout=30000)
    except Exception:
        # Some pages lazy-load; give networkidle chance
        await page.wait_for_load_state("networkidle")


async def parse_job_card(card) -> Dict[str, Any]:
    # JobsDB uses article/job cards; use robust queries
    title = await card.locator("a[aria-label], a[title]").first.text_content() or ""
    title = title.strip()
    job_url = None
    try:
        href = await card.locator("a[aria-label], a[title]").first.get_attribute("href")
        if href and href.startswith("/"):
            job_url = BASE_URL + href
        else:
            job_url = href
    except Exception:
        pass

    company_name = (await card.locator("[data-automation='jobCompany'], [data-automation='job-card-company-name'], .company").first.text_content() or "").strip()
    location = (await card.locator("[data-automation='jobLocation'], .job-location").first.text_content() or "").strip()
    posted_time = (await card.locator("[data-automation='jobListingDate'] time, [data-automation='jobListingDate'], time, .job-date").first.text_content() or "").strip()

    # Company profile link if present
    company_profile_url = None
    try:
        comp_link = await card.locator("a[href*='/companies/'], a[href*='/company/']").first.get_attribute("href")
        if comp_link:
            company_profile_url = comp_link if comp_link.startswith("http") else f"{BASE_URL}{comp_link}"
    except Exception:
        pass

    return {
        "job_title": title,
        "job_url": job_url,
        "company_name": company_name,
        "company_profile_url": company_profile_url,
        "location": location,
        "posted_time": posted_time,
    }


async def extract_listings_on_page(page: Page) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    # General card selector on JobsDB
    # Prefer JobsDB data-automation attributes
    cards = page.locator("[data-automation='job-card'], article")
    count = await cards.count()
    for i in range(count):
        card = cards.nth(i)
        try:
            item = await parse_job_card(card)
            if item.get("job_title"):
                records.append(item)
        except Exception:
            continue
    return records


async def goto_next_page(page: Page) -> bool:
    # With direct URL strategy, we handle pagination by URL outside this function
    # Keep a minimal fallback using rel=next if present
    try:
        rel_next = page.locator("a[rel='next']")
        if await rel_next.count() > 0:
            await rel_next.first.click()
            await page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass
    return False


async def enrich_company_info(page: Page, company_url: Optional[str]) -> Dict[str, Optional[str]]:
    if not company_url:
        return {"industry": None, "founded": None}
    try:
        await page.goto(company_url, timeout=60000)
        await ensure_page_ready(page)
        # Look for text fields that often appear on company profile pages
        industry = None
        founded = None
        try:
            # heuristic selectors/texts
            industry_el = page.locator("text=/Industry/i, [data-qa='industry']").first
            if await industry_el.count() > 0:
                # try to get following sibling value
                container = industry_el.locator("xpath=ancestor::*[1]")
                text = (await container.text_content() or "").strip()
                industry = text.split(":")[-1].strip() if ":" in text else text
        except Exception:
            pass
        try:
            founded_el = page.locator("text=/Founded/i, text=/Founded in/i, [data-qa='founded']").first
            if await founded_el.count() > 0:
                container = founded_el.locator("xpath=ancestor::*[1]")
                text = (await container.text_content() or "").strip()
                founded = text.split(":")[-1].strip() if ":" in text else text
        except Exception:
            pass
        return {"industry": industry, "founded": founded}
    except Exception:
        return {"industry": None, "founded": None}


async def scrape_keyword(keyword: str, max_pages: int = 1) -> List[JobRecord]:
    results: List[JobRecord] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        # Separate page for enrichment to avoid losing list state
        enrich_page = await context.new_page()

        for page_num in range(1, max_pages + 1):
            await goto_search_results(page, keyword, page_num)
            listings = await extract_listings_on_page(page)
            for item in listings:
                # enrichment = await enrich_company_info(enrich_page, item.get("company_profile_url"))
                enrichment = {"industry": None, "founded": None}
                results.append(
                    JobRecord(
                        keyword=keyword,
                        job_title=item.get("job_title") or "",
                        company_name=item.get("company_name") or "",
                        company_profile_url=item.get("company_profile_url"),
                        location=item.get("location"),
                        posted_time=item.get("posted_time"),
                        job_url=item.get("job_url"),
                        industry=enrichment.get("industry"),
                        founded=enrichment.get("founded"),
                    )
                )

        await context.close()
        await browser.close()
    return results


def save_outputs(records: List[JobRecord], base_filename: str = "jobsdb_hk_jobs") -> None:
    if not records:
        print("No records to save.")
        return
    df = pd.DataFrame([asdict(r) for r in records])
    csv_path = f"{base_filename}.csv"
    xlsx_path = f"{base_filename}.xlsx"
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    print(f"Saved {len(df)} rows to {csv_path} and {xlsx_path}")


async def main():
    all_records: List[JobRecord] = []
    for kw in TARGET_KEYWORDS:
        print(f"Scraping keyword: {kw}")
        try:
            recs = await scrape_keyword(kw, max_pages=3)
            all_records.extend(recs)
        except Exception as e:
            print(f"Failed to scrape {kw}: {e}")
    save_outputs(all_records)


if __name__ == "__main__":
    asyncio.run(main())

    