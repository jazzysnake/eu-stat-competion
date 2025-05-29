import aiofiles
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig


class Crawler:
    """A wrapper around AsyncWebCrawler for simplified web crawling."""

    def __init__(
        self,
        request_timeout_sec: int = 5,
    ) -> None:
        """Initializes the Crawler with a headless Chrome browser configuration.

        Args:
            request_timeout_sec: Timeout for web page requests in seconds.
                                 Defaults to 5.
        """
        self.conf = BrowserConfig(
            browser_type='chrome',
            headless=True,
            text_mode=False,
        )
        self.run_cfg = CrawlerRunConfig(page_timeout=request_timeout_sec * 1000, magic=True)
        self.crawler = AsyncWebCrawler(config=self.conf)

    async def crawl(self, url: str):
        """Crawls the given URL using AsyncWebCrawler.

        Args:
            url: The URL of the website to crawl.

        Returns:
            crawl4ai.crawler.CrawledInfo: The result of the crawl operation,
            containing HTML, cleaned HTML, markdown, etc.
        """
        return await self.crawler.arun(url, config=self.run_cfg)

    async def close(self) -> None:
        """Closes the underlying web crawler and browser instance."""
        await self.crawler.close()


class HTMLDownloader:
    """Downloads HTML content from URLs and saves it to files."""

    def __init__(
        self,
    ) -> None:
        """Initializes the HTMLDownloader with a Crawler instance."""
        self.crawler = Crawler()

    async def download(self, url: str, filename: str) -> None:
        """Downloads the cleaned HTML content of a URL and saves it to a file.

        Args:
            url: The URL to download HTML from.
            filename: The name of the file to save the cleaned HTML content to.
        """
        res = await self.crawler.crawl(url)
        async with aiofiles.open(filename, 'w') as f:
            await f.write(res.cleaned_html)


if __name__ == '__main__':
    import asyncio

    crawler = Crawler()
    res = asyncio.run(crawler.crawl('https://www.adeccogroup.com/investors/annual-report'))
    with open('scratchpad/page.html', 'w') as f:
        print(res.html, file=f)
    with open('scratchpad/page-cleaned.html', 'w') as f:
        print(res.cleaned_html, file=f)
    with open('scratchpad/markdown.html', 'w') as f:
        print(res.markdown, file=f)
