import httpx
import urllib.parse as urlp
import aiofiles
from typing import Optional, Dict, Any


class PDFDownloader:
    """
    A class to download PDF files using httpx, supporting both sync and async.

    Manages httpx clients for potential reuse across multiple downloads and
    saves files to a specified download folder.

    Recommended usage is via context managers ('with' or 'async with') to ensure
    clients are properly closed.

    Attributes:
        download_folder (Path): The absolute path to the folder where PDFs will be saved.
        sync_client (httpx.Client): The synchronous httpx client instance.
        async_client (httpx.AsyncClient): The asynchronous httpx client instance.
    """

    def __init__(
        self,
        default_headers: Optional[Dict[str, str]] = None,
        client_options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initializes the PDFDownloader.

        Args:
            download_folder: The path (string or Path object) to the directory
                             where downloaded PDFs should be saved. Defaults to "pdf_downloads".
                             The directory will be created if it doesn't exist.
            default_headers: Optional dictionary of headers to use for all requests.
                             A default User-Agent is added if not provided.
            client_options: Optional dictionary of keyword arguments to pass to
                            both httpx.Client and httpx.AsyncClient constructors
                            (e.g., timeout, limits, proxies).
        """
        effective_client_options = client_options or {}
        effective_headers = {**(default_headers or {})}
        effective_client_options.setdefault('follow_redirects', True)
        effective_client_options.setdefault('timeout', 30.0)  # Default timeout

        # Initialize clients
        self.sync_client = httpx.Client(
            headers=effective_headers,
            **effective_client_options,
        )
        self.async_client = httpx.AsyncClient(
            headers=effective_headers,
            **effective_client_options,
        )
        self._chunk_size = 8192  # Internal chunk size for streaming

    async def is_pdf(
        self,
        url: str,
    ) -> bool:
        parsed = urlp.urlparse(url)
        if parsed.path.endswith('.pdf'):
            return True
        res = await self.async_client.get(url)
        res.raise_for_status()
        return 'application/pdf' in res.headers.get('Content-Type')

    def download_sync(
        self,
        url: str,
        filename: str,
    ) -> None:
        """
        Downloads a PDF from a URL synchronously using streaming.

        Args:
            url: The URL of the PDF file to download.
            filename: The desired name for the saved file (including extension).

        Returns:
            The Path object of the downloaded file if successful, None otherwise.
        """

        with self.sync_client.stream('GET', url) as response:
            response.raise_for_status()

            with open(filename, 'wb') as f:
                for chunk in response.iter_bytes(chunk_size=self._chunk_size):
                    f.write(chunk)

    async def download_async(
        self,
        url: str,
        filename: str,
        spoof_browser_user_agent: bool = False,
    ) -> None:
        """
        Downloads a PDF from a URL asynchronously using streaming.

        Args:
            url: The URL of the PDF file to download.
            filename: The desired name for the saved file (including extension).

        Returns:
            The path of the downloaded file if successful, None otherwise.
        """
        headers = (
            None
            if not spoof_browser_user_agent
            else {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
            }
        )
        async with self.async_client.stream('GET', url, headers=headers) as response:
            response.raise_for_status()

            async with aiofiles.open(filename, 'wb') as f:
                async for chunk in response.aiter_bytes(chunk_size=self._chunk_size):
                    await f.write(chunk)

    def __enter__(self) -> 'PDFDownloader':
        """Enter the synchronous context manager."""
        # Client is already initialized, just return self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the synchronous context manager, closing the sync client."""
        self.sync_client.close()

    async def __aenter__(self) -> 'PDFDownloader':
        """Enter the asynchronous context manager."""
        # Client is already initialized, just return self
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the asynchronous context manager, closing the async client."""
        await self.async_client.aclose()
