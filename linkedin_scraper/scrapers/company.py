"""
Company scraper for LinkedIn.

Extracts company information from LinkedIn company pages.
"""
import logging
from typing import Optional
from playwright.async_api import Page

from ..models.company import Company
from ..core.exceptions import ProfileNotFoundError
from ..callbacks import ProgressCallback, SilentCallback
from .base import BaseScraper

logger = logging.getLogger(__name__)


class CompanyScraper(BaseScraper):
    """
    Scraper for LinkedIn company pages.
    
    Example:
        async with BrowserManager() as browser:
            scraper = CompanyScraper(browser.page)
            company = await scraper.scrape("https://www.linkedin.com/company/microsoft/")
            print(company.to_json())
    """
    
    def __init__(self, page: Page, callback: Optional[ProgressCallback] = None):
        """
        Initialize company scraper.

        Args:
            page: Playwright page object
            callback: Optional progress callback
        """
        super().__init__(page, callback or SilentCallback())

    def _overview_field_count(self, overview: dict) -> int:
        """Return number of non-null, non-empty overview fields (excluding about_us for threshold)."""
        return sum(
            1 for k, v in overview.items()
            if v is not None and str(v).strip()
        )

    async def _get_overview_from_main_page(self) -> dict:
        """
        Try to extract overview (about_us, industry, company_size, etc.) from the current
        company main page without navigating to /about/. Returns same dict shape as _get_overview.
        """
        overview = {
            "about_us": None,
            "website": None,
            "phone": None,
            "headquarters": None,
            "founded": None,
            "industry": None,
            "company_type": None,
            "company_size": None,
            "specialties": None,
        }
        try:
            # Try section with "Overview" or "About" (main page often has sidebar or block)
            for heading in ("Overview", "About", "About us"):
                section_loc = self.page.locator(f'section:has(h2:has-text("{heading}"))')
                if await section_loc.count() == 0:
                    section_loc = self.page.locator(f'section:has(h3:has-text("{heading}"))')
                if await section_loc.count() == 0:
                    continue
                section = section_loc.first

                # About-us: first substantial paragraph
                about_p = section.locator("p.break-words.text-body-medium").first
                if await about_p.count() == 0:
                    about_p = section.locator("p").first
                if await about_p.count() > 0:
                    text = (await about_p.inner_text()).strip()
                    if len(text) > 20:
                        overview["about_us"] = text

                # Definition list (same structure as /about/ page)
                dl_loc = section.locator("dl.overflow-hidden").first
                if await dl_loc.count() == 0:
                    dl_loc = section.locator("dl").first
                if await dl_loc.count() > 0:
                    dl = dl_loc
                    dt_elements = await dl.locator("dt").all()
                    for dt in dt_elements:
                        label_elem = dt.locator("h3.text-heading-medium").first
                        if await label_elem.count() == 0:
                            label_elem = dt.locator(".text-heading-medium").first
                        if await label_elem.count() == 0:
                            continue
                        label = (await label_elem.inner_text()).strip().lower()
                        if "verified page" in label:
                            continue
                        dd = dt.locator("xpath=following-sibling::dd[1]").first
                        if await dd.count() == 0:
                            continue
                        if "website" in label:
                            link = dd.locator("a[href]").first
                            if await link.count() > 0:
                                href = await link.get_attribute("href")
                                if href and "linkedin.com" not in href:
                                    overview["website"] = href.strip()
                            if overview["website"] is None:
                                overview["website"] = (await dd.inner_text()).strip()
                            continue
                        value = (await dd.inner_text()).strip()
                        if not value:
                            continue
                        if "industry" in label:
                            overview["industry"] = value
                        elif "company size" in label or "size" in label:
                            overview["company_size"] = value
                        elif "headquarters" in label or "location" in label:
                            overview["headquarters"] = value
                        elif "specialt" in label:
                            overview["specialties"] = value
                        elif "founded" in label:
                            overview["founded"] = value
                        elif ("company type" in label or "type" in label) and "verified" not in label:
                            overview["company_type"] = value
                        elif "phone" in label:
                            overview["phone"] = value
                break
        except Exception as e:
            logger.debug(f"Error getting overview from main page: {e}")
        return overview
    
    async def scrape(self, linkedin_url: str, skip_about_nav: bool = False) -> Company:
        """
        Scrape a LinkedIn company page.

        Args:
            linkedin_url: URL of the LinkedIn company page
            skip_about_nav: If True, never navigate to /about/; use only main-page overview when available.

        Returns:
            Company object with scraped data

        Raises:
            ProfileNotFoundError: If company page not found
        """
        logger.info(f"Starting company scraping: {linkedin_url}")
        await self.callback.on_start("company", linkedin_url)

        # Navigate to company page
        await self.navigate_and_wait(linkedin_url)
        await self.callback.on_progress("Navigated to company page", 10)

        # Check if page exists
        await self.check_rate_limit()

        # Extract basic info
        name = await self._get_name()
        await self.callback.on_progress(f"Got company name: {name}", 20)

        # Try overview from main page first; skip /about/ if good enough or if skip_about_nav
        overview = await self._get_overview_from_main_page()
        use_main_only = skip_about_nav or self._overview_field_count(overview) >= 2
        if not use_main_only:
            overview = await self._get_overview(linkedin_url)
        await self.callback.on_progress("Got overview details", 50)
        
        # Create company object
        company = Company(
            linkedin_url=linkedin_url,
            name=name,
            about_us=overview.pop('about_us', None),
            **overview
        )
        
        await self.callback.on_progress("Scraping complete", 100)
        await self.callback.on_complete("company", company)
        
        logger.info(f"Successfully scraped company: {name}")
        return company
    
    async def _get_name(self) -> str:
        """Extract company name."""
        try:
            # Try main heading
            name_elem = self.page.locator('h1').first
            name = await name_elem.inner_text()
            return name.strip()
        except Exception as e:
            logger.warning(f"Error getting company name: {e}")
            return "Unknown Company"
    
    async def _get_about(self) -> Optional[str]:
        """Extract about/description section."""
        try:
            # Look for "About us" section
            sections = await self.page.locator('section').all()
            
            for section in sections:
                section_text = await section.inner_text()
                if any(s in section_text[:100] for s in ('Overview', 'About', 'About us')):
                    # Get the content paragraph
                    paragraphs = await section.locator('p').all()
                    if paragraphs:
                        about = await paragraphs[0].inner_text()
                        return about.strip()
            
            return None
        except Exception as e:
            logger.debug(f"Error getting about section: {e}")
            return None
    
    async def _get_overview(self, linkedin_url: str) -> dict:
        """
        Navigate to company /about/ page and extract overview details.

        Returns dict with: about_us, website, phone, headquarters, founded, industry,
        company_type, company_size, specialties
        """
        overview = {
            "about_us": None,
            "website": None,
            "phone": None,
            "headquarters": None,
            "founded": None,
            "industry": None,
            "company_type": None,
            "company_size": None,
            "specialties": None
        }

        try:
            about_url = linkedin_url.rstrip("/") + "/about/"
            await self.navigate_and_wait(about_url)

            # Overview section: section containing h2 "Overview"
            section_loc = self.page.locator('section:has(h2:has-text("Overview"))')
            if await section_loc.count() == 0:
                return overview
            section = section_loc.first

            # About-us: first paragraph with break-words / text-body-medium in that section
            about_p = section.locator("p.break-words.text-body-medium").first
            if await about_p.count() > 0:
                overview["about_us"] = (await about_p.inner_text()).strip()

            # Definition list in same section
            dl_loc = section.locator("dl.overflow-hidden")
            if await dl_loc.count() == 0:
                return overview
            dl = dl_loc.first

            dt_elements = await dl.locator("dt").all()
            for dt in dt_elements:
                label_elem = dt.locator("h3.text-heading-medium").first
                if await label_elem.count() == 0:
                    label_elem = dt.locator(".text-heading-medium").first
                if await label_elem.count() == 0:
                    continue
                label = (await label_elem.inner_text()).strip().lower()
                if "verified page" in label:
                    continue

                dd = dt.locator("xpath=following-sibling::dd[1]").first
                if await dd.count() == 0:
                    continue

                # Website: prefer href from link inside dd
                if "website" in label:
                    link = dd.locator("a[href]").first
                    if await link.count() > 0:
                        href = await link.get_attribute("href")
                        if href and "linkedin.com" not in href:
                            overview["website"] = href.strip()
                    if overview["website"] is None:
                        overview["website"] = (await dd.inner_text()).strip()
                    continue

                value = (await dd.inner_text()).strip()
                if not value:
                    continue

                if "industry" in label:
                    overview["industry"] = value
                elif "company size" in label or "size" in label:
                    overview["company_size"] = value
                elif "headquarters" in label or "location" in label:
                    overview["headquarters"] = value
                elif "specialt" in label:
                    overview["specialties"] = value
                elif "founded" in label:
                    overview["founded"] = value
                elif ("company type" in label or "type" in label) and "verified" not in label:
                    overview["company_type"] = value
                elif "phone" in label:
                    overview["phone"] = value

        except Exception as e:
            logger.debug(f"Error getting company overview: {e}")

        return overview
