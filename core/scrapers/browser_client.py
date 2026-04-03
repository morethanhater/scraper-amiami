from json import loads as json_loads
from typing import Dict, List, Optional
from urllib.parse import urlencode

from playwright.sync_api import BrowserContext, Page, sync_playwright


class BrowserJsonClient:
    def __init__(
        self,
        start_url: str,
        browser_channel: str,
        headless: bool,
        initial_cookies: Optional[List[Dict]] = None,
    ):
        self.start_url = start_url
        self.browser_channel = browser_channel
        self.headless = headless
        self.initial_cookies = initial_cookies or []
        self._playwright = None
        self._browser = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    def __enter__(self):
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            channel=self.browser_channel,
            headless=self.headless,
        )
        self._context = self._browser.new_context()
        if self.initial_cookies:
            self._context.add_cookies(self.initial_cookies)
        self._page = self._context.new_page()
        self._page.goto(self.start_url, wait_until="domcontentloaded")
        self._page.wait_for_timeout(3000)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._context is not None:
            try:
                self._context.close()
            except Exception:
                pass
            finally:
                self._context = None

        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:
                pass
            finally:
                self._browser = None

        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:
                pass
            finally:
                self._playwright = None

        self._page = None

    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Browser page is not initialized")
        return self._page

    def get_json(self, url: str, params: Dict[str, str], headers: Dict[str, str]) -> Dict:
        response = self._fetch(url, params, headers)
        if response["status"] == 403 and "Just a moment" in response["text"]:
            self._solve_challenge(url, params)
            response = self._fetch(url, params, headers)
        if response["status"] == 403 and "Sorry, you have been blocked" in response["text"]:
            raise RuntimeError(
                "HTTP Error 403: AmiAmi blocked this browser session. "
                "Wait for the block to clear, lower concurrency, and rerun with a visible browser."
            )

        if response["status"] >= 400:
            raise RuntimeError(f"HTTP Error {response['status']}: {response['text'][:200]}")

        return json_loads(response["text"])

    def export_cookies(self) -> List[Dict]:
        if self._context is None:
            raise RuntimeError("Browser context is not initialized")
        return self._context.cookies()

    def get_user_agent(self) -> str:
        return self.page.evaluate("() => navigator.userAgent")

    def _fetch(self, url: str, params: Dict[str, str], headers: Dict[str, str]) -> Dict[str, str]:
        if self._context is None:
            raise RuntimeError("Browser context is not initialized")

        browser_headers = {
            key: value for key, value in headers.items() if key.lower() != "user-agent"
        }
        self._context.set_extra_http_headers(browser_headers)
        target_url = f"{url}?{urlencode(params)}"
        response = self.page.goto(target_url, wait_until="domcontentloaded")
        self.page.wait_for_timeout(1000)

        if response is None:
            return {"status": 0, "text": self.page.content()}

        text = self.page.locator("body").inner_text()
        return {"status": response.status, "text": text}

    def _solve_challenge(self, url: str, params: Dict[str, str]):
        target_url = f"{url}?{urlencode(params)}"
        print("Cloudflare challenge detected.")
        print("A browser window has been opened. Complete any verification there, then press Enter here.")
        self.page.goto(target_url, wait_until="domcontentloaded")
        input()
        self.page.goto(self.start_url, wait_until="domcontentloaded")
        self.page.wait_for_timeout(1000)
