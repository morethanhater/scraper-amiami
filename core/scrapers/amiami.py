from concurrent.futures import ThreadPoolExecutor, as_completed
from json import load as json_load
from math import ceil
from os.path import exists, join
from re import search as re_search
from time import sleep
from threading import Lock, local
from typing import Dict, List, Optional, Tuple

from config import (
    AMIAMI_API_ROOT,
    AMIAMI_BROWSER_CHANNEL,
    AMIAMI_CRAWL_SLEEP_SECONDS,
    AMIAMI_DETAIL_SLEEP_SECONDS,
    AMIAMI_DETAIL_WORKERS,
    AMIAMI_ENRICH_SAVE_EVERY,
    AMIAMI_FETCH_PREOWNED_DETAILS,
    AMIAMI_HEADLESS,
    AMIAMI_MAX_RETRIES,
    AMIAMI_PAGE_WORKERS,
    AMIAMI_RETRY_BASE_SECONDS,
    AMIAMI_START_URL,
    AMIAMI_TRANSPORT,
    AMIAMI_USER_AGENT,
    AMIAMI_USER_KEY,
    BROWSER,
    DATA_LIST_FILE,
    ITEMS_PER_PAGE,
    OUTPUT_DIR,
    WEB_DATA_DIR,
    AmiAmiCodeTypeLiteral,
)
from curl_cffi import requests
from models.amiami.enums import (
    ItemSortingEnum,
    ItemTypeEnum,
)
from models.amiami.index import (
    AmiAmiItem,
    AmiAmiItemOutput,
    AmiAmiItemResponse,
    AmiAmiItemsResponse,
)
from models.amiami.utils import AmiAmiItemOutputDump, AmiAmiItemsDump, AmiAmiQueryArgs
from scrapers.browser_client import BrowserJsonClient
from utils.date_util import get_current_date
from utils.json_util import save_model_to_json


class AmiAmiScraper:
    def __init__(
        self,
        always_scrap_details: bool = False,
        stop_on_429: bool = True,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        """
        Main class for scraping AmiAmi

        Args:
            always_scrap_details (bool, optional): If True, scrap item details to get more data.
                Note: Always the case for pre-owned items.
                Defaults to False.
            stop_on_429 (bool, optional): If True, will stop the program if an error 429 occurs.
                Note: The other case will result on items enriched with only basic crawled data.
                Defaults to True.
            extra_headers (Optional[Dict[str, str]], optional): Extra request headers.
                Defaults to None.
        """
        self.always_scrap_details = always_scrap_details
        self.stop_on_429 = stop_on_429
        self.headers = {
            "X-User-Key": AMIAMI_USER_KEY,
            "User-Agent": AMIAMI_USER_AGENT,
        }
        if extra_headers is not None:
            self.headers.update(extra_headers)
        self.crawl_sleep_time = AMIAMI_CRAWL_SLEEP_SECONDS
        self.scrap_sleep_time = AMIAMI_DETAIL_SLEEP_SECONDS
        self.fetch_preowned_details = AMIAMI_FETCH_PREOWNED_DETAILS
        self.page_workers = max(1, AMIAMI_PAGE_WORKERS)
        self.detail_workers = max(1, AMIAMI_DETAIL_WORKERS)
        self.max_retries = max(0, AMIAMI_MAX_RETRIES)
        self.retry_base_seconds = max(0.5, AMIAMI_RETRY_BASE_SECONDS)
        self.enrich_save_every = max(1, AMIAMI_ENRICH_SAVE_EVERY)
        self.transport = AMIAMI_TRANSPORT
        self.browser_client: Optional[BrowserJsonClient] = None
        self.detail_enrichment_disabled = False
        self._detail_thread_local = local()
        self._detail_browser_clients: List[BrowserJsonClient] = []
        self._detail_browser_clients_lock = Lock()
        self._detail_browser_closed_warning_shown = False
        self._detail_browser_closed_during_run = False

    def __enter__(self):
        if self.transport == "browser":
            self.browser_client = BrowserJsonClient(
                start_url=AMIAMI_START_URL,
                browser_channel=AMIAMI_BROWSER_CHANNEL,
                headless=AMIAMI_HEADLESS,
            )
            self.browser_client.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.browser_client is not None:
            self.browser_client.__exit__(exc_type, exc, tb)

    def _normalize_params(self, params: Dict[str, str]) -> Dict[str, str]:
        return {
            key: str(getattr(value, "value", value)) for key, value in params.items()
        }

    def _build_items_params(self, page: int, args: AmiAmiQueryArgs) -> Dict[str, str]:
        params = {
            "pagecnt": page,
            "pagemax": ITEMS_PER_PAGE,
            "lang": "eng",
            "age_confirm": 1,
            "s_keywords": args.keyword or "",
            "s_cate1": args.category1 if args.category1 else "",
            "s_cate2": args.category2 if args.category2 else "",
            "s_cate3": args.category3 if args.category3 else "",
            "s_sortkey": args.sort_key if args.sort_key else "",
            # "mcode": "",
            # "ransu": "",
            # "s_cate_tag": 14
        }
        for item_type in args.types:
            params[item_type] = "1"
        return params

    def _build_item_details_params(
        self,
        code: str,
        code_type: AmiAmiCodeTypeLiteral,
    ) -> Dict[str, str]:
        return {code_type: code}

    def _export_browser_cookies(self) -> Optional[List[Dict]]:
        if self.transport == "browser" and self.browser_client is not None:
            return self.browser_client.export_cookies()
        return None

    def _build_output_filename(self, timestamp: str, args: AmiAmiQueryArgs) -> str:
        return f"{timestamp}-scrapped_items.json"

    def _build_mapped_filename(self, timestamp: str) -> str:
        return f"{timestamp}-mapped_items.json"

    def _build_raw_output_path(self, filename: str) -> str:
        return join(OUTPUT_DIR, filename)

    def _build_mapped_output_path(self, filename: str) -> str:
        return join(WEB_DATA_DIR, filename)

    def _request_json(self, url: str, params: Dict[str, str]) -> Dict:
        normalized_params = self._normalize_params(params)
        if self.transport == "browser":
            if self.browser_client is None:
                raise RuntimeError("Browser transport is not initialized")
            return self._with_retry(
                lambda: self.browser_client.get_json(url, normalized_params, self.headers),
                context=f"{url} {normalized_params.get('pagecnt', normalized_params)}",
            )

        return self._with_retry(
            lambda: self._request_json_direct(url, normalized_params),
            context=f"{url} {normalized_params.get('pagecnt', normalized_params)}",
        )

    def _request_json_direct(self, url: str, normalized_params: Dict[str, str]) -> Dict:
        response = requests.get(
            url,
            params=normalized_params,
            headers=self.headers,
            impersonate=BROWSER,
        )
        print(f"Request status: {response.status_code}")
        response.raise_for_status()
        return response.json()

    def _request_json_http(
        self,
        url: str,
        params: Dict[str, str],
        extra_headers: Optional[Dict[str, str]] = None,
        cookies: Optional[List[Dict]] = None,
    ) -> Dict:
        normalized_params = self._normalize_params(params)
        headers = dict(self.headers)
        if extra_headers is not None:
            headers.update(extra_headers)

        session = requests.Session(headers=headers, impersonate=BROWSER)
        if cookies:
            for cookie in cookies:
                session.cookies.set(
                    cookie["name"],
                    cookie["value"],
                    domain=cookie.get("domain"),
                    path=cookie.get("path"),
                )

        return self._with_retry(
            lambda: self._request_json_http_once(session, url, normalized_params),
            context=f"{url} {normalized_params.get('pagecnt', normalized_params)}",
        )

    def _request_json_http_once(
        self,
        session: requests.Session,
        url: str,
        normalized_params: Dict[str, str],
    ) -> Dict:
        response = session.get(url, params=normalized_params)
        response.raise_for_status()
        return response.json()

    def _with_retry(self, fn, context: str) -> Dict:
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                return fn()
            except Exception as exc:
                last_error = exc
                error_text = str(exc)
                if "429" not in error_text or attempt >= self.max_retries:
                    raise

                delay = self.retry_base_seconds * (2**attempt)
                print(
                    f"HTTP 429 on {context}. Retry {attempt + 1}/{self.max_retries} in {delay:.1f}s..."
                )
                sleep(delay)

        raise last_error

    def _crawl_items_on_page(
        self,
        page: int,
        args: AmiAmiQueryArgs,
    ) -> AmiAmiItemsResponse:
        """
        Crawl all items on a given page.

        Args:
            page (int): Page number.
            args (AmiAmiQueryArgs): Request args.

        Returns:
            AmiAmiItemsResponse: Received data.
        """
        params = self._build_items_params(page, args)
        url = f"{AMIAMI_API_ROOT}/items"
        print(f"> Crawling '{url}' with params={params}")
        data = self._request_json(url, params)
        return AmiAmiItemsResponse(**data)

    def _resolve_default_sort_key(self, args: AmiAmiQueryArgs):
        if args.sort_key is not None:
            return

        if ItemTypeEnum.PRE_OWNED in args.types and len(args.types) == 1:
            args.sort_key = ItemSortingEnum.PREOWNED
        else:
            args.sort_key = ItemSortingEnum.RECENT_UPDATE

    def _get_total_pages(
        self,
        first_response: AmiAmiItemsResponse,
        args: AmiAmiQueryArgs,
    ) -> int:
        total_pages = ceil(first_response.search_result.total_results / ITEMS_PER_PAGE)
        if args.num_pages is not None:
            return min(total_pages, args.num_pages)
        return total_pages

    def _scrap_items_sequential(
        self,
        args: AmiAmiQueryArgs,
        total_pages: int,
    ) -> List[AmiAmiItem]:
        results: List[AmiAmiItem] = []
        for page in range(2, total_pages + 1):
            response = self._crawl_items_on_page(page, args)
            if not response.api_success or not response.items:
                break
            results.extend(response.items)
            sleep(self.crawl_sleep_time)
        return results

    def _get_parallel_items_context(self) -> Tuple[Dict[str, str], List[Dict]]:
        extra_headers: Dict[str, str] = {}
        cookies: List[Dict] = []
        if self.transport == "browser" and self.browser_client is not None:
            extra_headers["User-Agent"] = self.browser_client.get_user_agent()
            cookies = self.browser_client.export_cookies()
        return extra_headers, cookies

    def _build_page_batches(self, total_pages: int) -> List[List[int]]:
        page_numbers = list(range(2, total_pages + 1))
        page_batches = [
            page_numbers[index::self.page_workers] for index in range(self.page_workers)
        ]
        return [batch for batch in page_batches if batch]

    def _scrap_items_parallel(
        self,
        args: AmiAmiQueryArgs,
        total_pages: int,
    ) -> List[AmiAmiItem]:
        extra_headers, cookies = self._get_parallel_items_context()
        print(f"Fetching pages 2-{total_pages} with {self.page_workers} workers...")

        responses_by_page: Dict[int, AmiAmiItemsResponse] = {}
        page_batches = self._build_page_batches(total_pages)
        with ThreadPoolExecutor(max_workers=self.page_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_items_batch_parallel,
                    batch,
                    args,
                    extra_headers,
                    cookies,
                ): batch
                for batch in page_batches
            }
            for future in as_completed(futures):
                batch_responses = future.result()
                responses_by_page.update(batch_responses)

        results: List[AmiAmiItem] = []
        for page in range(2, total_pages + 1):
            response = responses_by_page.get(page)
            if response is None or not response.api_success or not response.items:
                break
            results.extend(response.items)
        return results

    def _scrap_items(self, args: AmiAmiQueryArgs) -> List[AmiAmiItem]:
        """
        Scrap all items according to query.

        Args:
            args (AmiAmiQueryArgs): Request args.

        Returns:
            List[AmiAmiItem]: List of raw items obtained.
        """
        self._resolve_default_sort_key(args)

        first_response = self._crawl_items_on_page(1, args)
        if not first_response.api_success or not first_response.items:
            return []

        results: List[AmiAmiItem] = list(first_response.items)
        total_pages = self._get_total_pages(first_response, args)

        if total_pages <= 1:
            return results

        if self.page_workers <= 1:
            results.extend(self._scrap_items_sequential(args, total_pages))
            return results

        results.extend(self._scrap_items_parallel(args, total_pages))
        return results

    def _fetch_items_batch_parallel(
        self,
        pages: List[int],
        args: AmiAmiQueryArgs,
        extra_headers: Dict[str, str],
        cookies: List[Dict],
    ) -> Dict[int, AmiAmiItemsResponse]:
        responses: Dict[int, AmiAmiItemsResponse] = {}
        with self._parallel_items_client(cookies) as client:
            for page in pages:
                params = self._build_items_params(page, args)
                data = self._request_items_page_parallel(params, extra_headers, cookies, client)
                responses[page] = AmiAmiItemsResponse(**data)
                print(f"Fetched page {page}")
        return responses

    def _parallel_items_client(self, cookies: List[Dict]):
        if self.transport != "browser":
            return _NullContext()
        return BrowserJsonClient(
            start_url=AMIAMI_START_URL,
            browser_channel=AMIAMI_BROWSER_CHANNEL,
            headless=AMIAMI_HEADLESS,
            initial_cookies=cookies,
        )

    def _request_items_page_parallel(
        self,
        params: Dict[str, str],
        extra_headers: Dict[str, str],
        cookies: List[Dict],
        client,
    ) -> Dict:
        url = f"{AMIAMI_API_ROOT}/items"
        if self.transport == "browser":
            return client.get_json(url, self._normalize_params(params), self.headers)
        return self._request_json_http(
            url,
            params,
            extra_headers=extra_headers,
            cookies=cookies,
        )

    def _crawl_item_details(
        self,
        code: str,
        code_type: AmiAmiCodeTypeLiteral,
        browser_client: Optional[BrowserJsonClient] = None,
    ) -> AmiAmiItemResponse:
        """
        Crawl details page for the given item.

        Args:
            code (str): Item code.
            code_type (AmiAmiCodeTypeLiteral): Item code type.

        Returns:
            AmiAmiItemResponse: Received data.
        """
        params = self._build_item_details_params(code, code_type)
        url = f"{AMIAMI_API_ROOT}/item"
        if browser_client is not None:
            normalized_params = self._normalize_params(params)
            data = self._with_retry(
                lambda: browser_client.get_json(url, normalized_params, self.headers),
                context=f"{url} {code}",
            )
        else:
            data = self._request_json(url, params)

        sleep(self.scrap_sleep_time)
        return AmiAmiItemResponse(**data)

    def _is_closed_browser_error(self, error: Exception) -> bool:
        error_text = str(error)
        return (
            "Target page, context or browser has been closed" in error_text
            or "Browser page is not initialized" in error_text
        )

    def _note_closed_browser_error(self):
        self._detail_browser_closed_during_run = True
        if self._detail_browser_closed_warning_shown:
            return

        print("Detail browser window was closed. Detail enrichment is being disabled for the rest of this run.")
        self._detail_browser_closed_warning_shown = True

    def _map_item_details_to_final(
        self,
        api_response: AmiAmiItemResponse,
    ) -> AmiAmiItemOutput:
        """
        Map the detailed item into its final enriched format.

        Args:
            api_response (AmiAmiItemResponse): API data.

        Returns:
            AmiAmiItemOutput: Final item.
        """
        item_tags_sources = (
            api_response.embedded_data.makers
            + api_response.embedded_data.series_titles
            + api_response.embedded_data.original_titles
            + api_response.embedded_data.character_names
        )
        final_item = api_response.item.minify()

        # Generate item tags
        final_item.tags = [item.name for item in item_tags_sources]

        # Retrieve Item and Box condition if possible (for pre-owned items)
        match = re_search(
            r"\(Pre-owned ITEM:([A-CJ][+-]?)/BOX:([ABC]|N)\)",
            api_response.item.sname,
        )
        if match:
            final_item.item_condition, final_item.box_condition = match.groups()

        return final_item

    def _scrap_item(
        self,
        code: str,
        code_type: AmiAmiCodeTypeLiteral,
        check_alts: bool = True,
        browser_client: Optional[BrowserJsonClient] = None,
    ) -> List[AmiAmiItemOutput]:
        """
        Scrap an item's details page and its related items.

        Args:
            code (str): Item code.
            code_type (AmiAmiCodeTypeLiteral): Item code type.
            check_alts (bool, optional): If True, will scrap the items related to the current one.
                Useful to get alternative pre-owned items.
                Defaults to True.

        Returns:
            List[AmiAmiItemOutput]: List of final items obtained.
        """
        results: List[AmiAmiItemOutput] = []

        # Crawl details for given item
        try:
            response = self._crawl_item_details(
                code,
                code_type,
                browser_client=browser_client,
            )
        except Exception as e:
            if "429" in str(e) and self.stop_on_429:
                raise Exception("HTTP 429, try again later")
            if self._is_closed_browser_error(e):
                self._note_closed_browser_error()
            else:
                print(e)
            return results
        if not response.api_success or not response.item:
            print(f"Error on '{code}', nothing found.")
            return results

        # Map item to final format
        results.append(self._map_item_details_to_final(response))
        if check_alts:
            # Crawl related items pages
            print("Checking related items...")
            for other_item in response.embedded_data.other_items:
                # Check_alts to false to avoid getting items twice (and entering an infinite loop)
                results.extend(
                    self._scrap_item(
                        other_item.scode,
                        "scode",
                        check_alts=False,
                        browser_client=browser_client,
                    )
                )

        return results

    def _enrich_item_with_details(
        self,
        item: AmiAmiItem,
        cookies: Optional[List[Dict]] = None,
        browser_client: Optional[BrowserJsonClient] = None,
    ) -> List[AmiAmiItemOutput]:
        if self.detail_enrichment_disabled:
            mapped_items = []
        else:
            try:
                mapped_items = self._scrap_item_with_optional_browser(
                    item,
                    cookies,
                    browser_client=browser_client,
                )
            except Exception as e:
                if self._is_closed_browser_error(e):
                    self._note_closed_browser_error()
                else:
                    print(f"Detail enrichment failed for {item.gcode}: {e}")
                    print("Disabling detail enrichment for the rest of this run.")
                self.detail_enrichment_disabled = True
                mapped_items = []

        if not mapped_items:
            mapped_items.append(item.minify())
            with open(join(OUTPUT_DIR, "_errors.txt"), "a") as f:
                f.write(
                    f"> {get_current_date()} - Error on gcode {item.gcode}\n",
                )

        for mapped_item in mapped_items:
            mapped_item.release_date = item.releasedate

        return mapped_items

    def _scrap_item_with_optional_browser(
        self,
        item: AmiAmiItem,
        cookies: Optional[List[Dict]],
        browser_client: Optional[BrowserJsonClient] = None,
    ) -> List[AmiAmiItemOutput]:
        if browser_client is not None:
            return self._scrap_item(
                item.gcode,
                "gcode",
                browser_client=browser_client,
            )
        if self.transport == "browser" and cookies is not None:
            with BrowserJsonClient(
                start_url=AMIAMI_START_URL,
                browser_channel=AMIAMI_BROWSER_CHANNEL,
                headless=AMIAMI_HEADLESS,
                initial_cookies=cookies,
            ) as client:
                return self._scrap_item(
                    item.gcode,
                    "gcode",
                    browser_client=client,
                )
        return self._scrap_item(item.gcode, "gcode")

    def _load_raw_items(self, filename: str) -> List[AmiAmiItem]:
        filepath = self._build_raw_output_path(filename)
        with open(filepath, "r", encoding="utf-8") as f:
            base_data_parsed = AmiAmiItemsDump(**json_load(f))
        return base_data_parsed.items

    def _load_existing_mapped_data(
        self,
        mapped_filepath: str,
    ) -> Tuple[int, List[AmiAmiItemOutput]]:
        try:
            with open(mapped_filepath, "r", encoding="utf-8") as f:
                result_data_parsed = AmiAmiItemOutputDump(**json_load(f))
            print("> Data retrieved from file")
            return result_data_parsed.current_index, result_data_parsed.items
        except FileNotFoundError:
            return -1, []

    def _save_checkpoint(
        self,
        mapped_filepath: str,
        current_index: int,
        result_mapped: List[AmiAmiItemOutput],
    ):
        with open(mapped_filepath, "w", encoding="utf-8") as f:
            save_model_to_json(
                f,
                AmiAmiItemOutputDump(
                    current_index=current_index,
                    items_length=len(result_mapped),
                    items=result_mapped,
                ),
            )

    def _should_fetch_item_details(self, item: AmiAmiItem) -> bool:
        return (
            not self.detail_enrichment_disabled
            and ((item.is_preowned and self.fetch_preowned_details) or self.always_scrap_details)
        )

    def _build_enrich_batches(
        self,
        amiami_items: List[AmiAmiItem],
        start_index: int,
    ) -> range:
        return range(start_index + 1, len(amiami_items), self.detail_workers)

    def _build_detail_job_batches(
        self,
        detail_jobs: List[Tuple[int, AmiAmiItem]],
    ) -> List[List[Tuple[int, AmiAmiItem]]]:
        if not detail_jobs:
            return []
        if self.detail_workers <= 1:
            return [detail_jobs]

        return [
            detail_jobs[index::self.detail_workers] for index in range(self.detail_workers)
            if detail_jobs[index::self.detail_workers]
        ]

    def _collect_enrich_batch_jobs(
        self,
        batch_start: int,
        batch_items: List[AmiAmiItem],
        total_items: int,
    ) -> Tuple[Dict[int, List[AmiAmiItemOutput]], List[Tuple[int, AmiAmiItem]]]:
        batch_results: Dict[int, List[AmiAmiItemOutput]] = {}
        detail_jobs: List[Tuple[int, AmiAmiItem]] = []

        for offset, item in enumerate(batch_items):
            index = batch_start + offset
            print(
                f"({index + 1}/{total_items}) On item {item.gcode}",
                f"https://www.amiami.com/eng/detail/?gcode={item.gcode}",
            )
            if self._should_fetch_item_details(item):
                detail_jobs.append((index, item))
            else:
                batch_results[index] = [item.minify()]

        return batch_results, detail_jobs

    def _run_detail_jobs(
        self,
        detail_jobs: List[Tuple[int, AmiAmiItem]],
        cookies: Optional[List[Dict]],
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> Dict[int, List[AmiAmiItemOutput]]:
        results: Dict[int, List[AmiAmiItemOutput]] = {}
        if not detail_jobs:
            return results

        if executor is not None and self.transport == "browser" and cookies is not None:
            futures = {
                executor.submit(self._run_detail_job_with_thread_client, index, item, cookies): (index, item)
                for index, item in detail_jobs
            }
            for future in as_completed(futures):
                index, item = futures[future]
                try:
                    result_index, mapped_items = future.result()
                    results[result_index] = mapped_items
                except Exception as e:
                    if self._is_closed_browser_error(e):
                        self._note_closed_browser_error()
                    else:
                        print(f"Detail worker batch failed near {item.gcode}: {e}")
                        print("Disabling detail enrichment for the rest of this run.")
                    self.detail_enrichment_disabled = True
                    results[index] = [item.minify()]
            return results

        detail_job_batches = self._build_detail_job_batches(detail_jobs)

        if self.detail_workers > 1:
            with ThreadPoolExecutor(max_workers=self.detail_workers) as executor:
                futures = {
                    executor.submit(self._run_detail_job_batch, batch, cookies): batch
                    for batch in detail_job_batches
                }
                for future in as_completed(futures):
                    batch = futures[future]
                    try:
                        results.update(future.result())
                    except Exception as e:
                        first_index, first_item = batch[0]
                        print(f"Detail worker batch failed near {first_item.gcode}: {e}")
                        print("Disabling detail enrichment for the rest of this run.")
                        self.detail_enrichment_disabled = True
                        for index, item in batch:
                            results[index] = [item.minify()]
            return results

        return self._run_detail_job_batch(detail_jobs, cookies)

    def _get_thread_browser_client(
        self,
        cookies: Optional[List[Dict]],
    ) -> BrowserJsonClient:
        client = getattr(self._detail_thread_local, "browser_client", None)
        if client is not None:
            return client

        client = BrowserJsonClient(
            start_url=AMIAMI_START_URL,
            browser_channel=AMIAMI_BROWSER_CHANNEL,
            headless=AMIAMI_HEADLESS,
            initial_cookies=cookies,
        )
        client.__enter__()
        self._detail_thread_local.browser_client = client
        with self._detail_browser_clients_lock:
            self._detail_browser_clients.append(client)
        return client

    def _run_detail_job_with_thread_client(
        self,
        index: int,
        item: AmiAmiItem,
        cookies: Optional[List[Dict]],
    ) -> Tuple[int, List[AmiAmiItemOutput]]:
        client = self._get_thread_browser_client(cookies)
        return index, self._enrich_item_with_details(
            item,
            browser_client=client,
        )

    def _close_detail_browser_clients(self):
        with self._detail_browser_clients_lock:
            clients = list(self._detail_browser_clients)
            self._detail_browser_clients.clear()

        for client in clients:
            try:
                client.__exit__(None, None, None)
            except Exception:
                pass

    def _run_detail_job_batch(
        self,
        detail_jobs: List[Tuple[int, AmiAmiItem]],
        cookies: Optional[List[Dict]],
    ) -> Dict[int, List[AmiAmiItemOutput]]:
        results: Dict[int, List[AmiAmiItemOutput]] = {}

        if self.transport == "browser" and cookies is not None:
            with BrowserJsonClient(
                start_url=AMIAMI_START_URL,
                browser_channel=AMIAMI_BROWSER_CHANNEL,
                headless=AMIAMI_HEADLESS,
                initial_cookies=cookies,
            ) as client:
                for index, item in detail_jobs:
                    results[index] = self._enrich_item_with_details(
                        item,
                        browser_client=client,
                    )
            return results

        for index, item in detail_jobs:
            results[index] = self._enrich_item_with_details(item, cookies)

        return results

    def _append_enrich_batch_results(
        self,
        batch_start: int,
        batch_items: List[AmiAmiItem],
        result_mapped: List[AmiAmiItemOutput],
        batch_results: Dict[int, List[AmiAmiItemOutput]],
        total_items: int,
        mapped_filepath: str,
    ):
        for offset, _item in enumerate(batch_items):
            index = batch_start + offset
            result_mapped.extend(batch_results[index])
            if (index + 1) % self.enrich_save_every == 0 or index == total_items - 1:
                print(f"Saving checkpoint at item {index + 1}...\n")
                self._save_checkpoint(mapped_filepath, index, result_mapped)

    def _register_mapped_filename(self, mapped_filename: str):
        if exists(DATA_LIST_FILE):
            with open(DATA_LIST_FILE, "r") as f:
                existing_files = set(f.read().splitlines())
        else:
            existing_files = set()

        with open(DATA_LIST_FILE, "a") as f:
            if mapped_filename not in existing_files:
                f.write(mapped_filename + "\n")

    def run_scraping(self, args: AmiAmiQueryArgs) -> Tuple[str, str]:
        """
        Main scraping method.
        Get all data from multiple pages according to a query.

        Args:
            args (AmiAmiQueryArgs): Request args.

        Returns:
            Tuple[str, str]: (timestamp, filename), where:
                - timestamp: Stringified date used in the raw data dump (acts as an ID)
                - filename: Full filename where the items were dumped
        """
        print("Run scraping...")
        results = self._scrap_items(args)

        print(f"Saving {len(results)} items...")
        timestamp = get_current_date()
        filename = self._build_output_filename(timestamp, args)
        with open(self._build_raw_output_path(filename), "w", encoding="utf-8") as f:
            save_model_to_json(
                f,
                AmiAmiItemsDump(items_length=len(results), items=results),
            )

        print(f"Data saved to '{filename}'")
        return timestamp, filename

    def run_enrich(self, timestamp: str, filename: str):
        """
        Main enriching method.
        Format raw items to final format.
        Can be relaunched from last saved checkpoint if an error occurred.

        Args:
            timestamp (str): Date used in the file to enrich.
            filename (str): Filename where raw data is located.
        """
        print("Run enrich...")
        amiami_items = self._load_raw_items(filename)
        mapped_filename = self._build_mapped_filename(timestamp)
        mapped_filepath = self._build_mapped_output_path(mapped_filename)
        start_index, result_mapped = self._load_existing_mapped_data(mapped_filepath)
        cookies = self._export_browser_cookies()
        detail_executor: Optional[ThreadPoolExecutor] = None

        try:
            if self.detail_workers > 1 and self.transport == "browser" and cookies is not None:
                detail_executor = ThreadPoolExecutor(max_workers=self.detail_workers)

            for batch_start in self._build_enrich_batches(amiami_items, start_index):
                batch_items = amiami_items[batch_start: batch_start + self.detail_workers]
                batch_results, detail_jobs = self._collect_enrich_batch_jobs(
                    batch_start,
                    batch_items,
                    len(amiami_items),
                )
                batch_results.update(
                    self._run_detail_jobs(
                        detail_jobs,
                        cookies,
                        executor=detail_executor,
                    )
                )
                self._append_enrich_batch_results(
                    batch_start,
                    batch_items,
                    result_mapped,
                    batch_results,
                    len(amiami_items),
                    mapped_filepath,
                )

            if len(amiami_items) == 0:
                self._save_checkpoint(mapped_filepath, start_index, result_mapped)
        finally:
            if detail_executor is not None:
                detail_executor.shutdown(wait=True)
            self._close_detail_browser_clients()

        if self._detail_browser_closed_during_run:
            print("Run finished after the detail browser window was closed. Remaining items were mapped without detail enrichment.")

        self._register_mapped_filename(mapped_filename)


class _NullContext:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False
