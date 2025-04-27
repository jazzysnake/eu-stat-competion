from crawl4ai import AsyncWebCrawler, BrowserConfig

class Crawler:
    def __init__(self) -> None:
        """Initializes the Crawler with a headless Chrome browser configuration."""
        self.conf = BrowserConfig(
            browser_type="chrome",
            headless=False,
            text_mode=True
        )
    async def crawl(self, url:str):
        """Crawls the given URL using AsyncWebCrawler.

        Args:
            url: The URL of the website to crawl.

        Returns:
            The content extracted from the crawled URL.
        """
        async with AsyncWebCrawler(config=self.conf) as crawler:
            result = await crawler.arun(url)
            return result
