import aiofiles
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

class Crawler:
    def __init__(
        self,
        request_timeout_sec: int = 5,
    ) -> None:
        """Initializes the Crawler with a headless Chrome browser configuration."""
        self.conf = BrowserConfig(
            browser_type="chrome",
            headless=True,
            text_mode=False,
        )
        self.run_cfg = CrawlerRunConfig(page_timeout=request_timeout_sec*1000)
        self.crawler = AsyncWebCrawler(config=self.conf)

    async def crawl(self, url:str):
        """Crawls the given URL using AsyncWebCrawler.

        Args:
            url: The URL of the website to crawl.

        Returns:
            The content extracted from the crawled URL.
        """
        return await self.crawler.arun(url, config=self.run_cfg)

    async def close(self) -> None:
        await self.crawler.close()

class HTMLDownloader:
    def __init__(
        self,
    ) -> None:
        self.crawler = Crawler()

    async def download(self, url: str, filename: str)-> None:
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
    with open('scratchpad/markdown.html','w') as f:
        print(res.markdown, file=f)
