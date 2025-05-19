import os
import aiofiles
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, ProxyConfig

class Crawler:
    def __init__(
        self,
        request_timeout_sec: int = 5,
        proxy_conf: ProxyConfig | None = None,
    ) -> None:
        """Initializes the Crawler with a headless Chrome browser configuration."""
        self.proxy_conf = proxy_conf
        self.conf = BrowserConfig(
            browser_type="chrome",
            headless=True,
            text_mode=False,
            user_agent_mode='random',
        )
        self.request_timeout_sec = request_timeout_sec
        self.run_cfg = CrawlerRunConfig(page_timeout=request_timeout_sec*1000)
        self.crawler = AsyncWebCrawler(config=self.conf)

    @staticmethod
    def read_proxy_from_env() -> ProxyConfig | None:
        host = os.environ.get('PROXY_HOST')
        port = os.environ.get('PROXY_PORT')
        user = os.environ.get('PROXY_USER')
        password = os.environ.get('PROXY_PW')
        if host is None or port is None:
            return None
        return ProxyConfig(
            server = f"http://{host}:{port}",
            username=user,
            password=password,
        )


    async def crawl(self, url:str, avoid_bot_detection: bool=False):
        """Crawls the given URL using AsyncWebCrawler.

        Args:
            url: The URL of the website to crawl.

        Returns:
            The content extracted from the crawled URL.
        """
        cfg = self.run_cfg
        if avoid_bot_detection:
            cfg = CrawlerRunConfig(
                page_timeout=self.request_timeout_sec*2*1000,
                magic=True,
                simulate_user=True,
                override_navigator=True,
                proxy_config=self.proxy_conf,
            )
        return await self.crawler.arun(url, config=cfg)

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
