from concurrent.futures import ThreadPoolExecutor, as_completed
from json import load as json_load
from math import ceil
from os.path import exists, join
from re import search as re_search
from time import sleep
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

    def _request_json(self, url: str, params: Dict[str, str]) -> Dict:
        normalized_params = {
            key: str(getattr(value, "value", value)) for key, value in params.items()
        }
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
        normalized_params = {
            key: str(getattr(value, "value", value)) for key, value in params.items()
        }
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
        for type in args.types:
            params[type] = "1"

        # Get items on given page
        url = f"{AMIAMI_API_ROOT}/items"
        print(f"> Crawling '{url}' with params={params}")
        data = self._request_json(url, params)
        return AmiAmiItemsResponse(**data)

    def _scrap_items(self, args: AmiAmiQueryArgs) -> List[AmiAmiItem]:
        """
        Scrap all items according to query.

        Args:
            args (AmiAmiQueryArgs): Request args.

        Returns:
            List[AmiAmiItem]: List of raw items obtained.
        """
        # Prepare sorting option
        if args.sort_key is None:
            if ItemTypeEnum.PRE_OWNED in args.types and len(args.types) == 1:
                args.sort_key = ItemSortingEnum.PREOWNED
            else:
                args.sort_key = ItemSortingEnum.RECENT_UPDATE

        # Crawl items in pages
        first_response = self._crawl_items_on_page(1, args)
        if not first_response.api_success or not first_response.items:
            return []

        results: List[AmiAmiItem] = list(first_response.items)
        total_pages = ceil(first_response.search_result.total_results / ITEMS_PER_PAGE)
        if args.num_pages is not None:
            total_pages = min(total_pages, args.num_pages)

        if total_pages <= 1:
            return results

        if self.page_workers <= 1:
            for page in range(2, total_pages + 1):
                response = self._crawl_items_on_page(page, args)
                if not response.api_success or not response.items:
                    break
                results.extend(response.items)
                sleep(self.crawl_sleep_time)
            return results

        extra_headers: Dict[str, str] = {}
        cookies: List[Dict] = []
        if self.transport == "browser" and self.browser_client is not None:
            extra_headers["User-Agent"] = self.browser_client.get_user_agent()
            cookies = self.browser_client.export_cookies()

        print(
            f"Fetching pages 2-{total_pages} with {self.page_workers} workers..."
        )
        responses_by_page: Dict[int, AmiAmiItemsResponse] = {}
        page_numbers = list(range(2, total_pages + 1))
        page_batches = [
            page_numbers[index::self.page_workers] for index in range(self.page_workers)
        ]
        page_batches = [batch for batch in page_batches if batch]
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

        for page in range(2, total_pages + 1):
            response = responses_by_page.get(page)
            if response is None or not response.api_success or not response.items:
                break
            results.extend(response.items)

        return results

    def _fetch_items_batch_parallel(
        self,
        pages: List[int],
        args: AmiAmiQueryArgs,
        extra_headers: Dict[str, str],
        cookies: List[Dict],
    ) -> Dict[int, AmiAmiItemsResponse]:
        responses: Dict[int, AmiAmiItemsResponse] = {}

        if self.transport == "browser":
            with BrowserJsonClient(
                start_url=AMIAMI_START_URL,
                browser_channel=AMIAMI_BROWSER_CHANNEL,
                headless=AMIAMI_HEADLESS,
                initial_cookies=cookies,
            ) as client:
                for page in pages:
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
                    }
                    for item_type in args.types:
                        params[item_type] = "1"

                    data = client.get_json(
                        f"{AMIAMI_API_ROOT}/items",
                        {
                            key: str(getattr(value, "value", value))
                            for key, value in params.items()
                        },
                        self.headers,
                    )
                    responses[page] = AmiAmiItemsResponse(**data)
                    print(f"Fetched page {page}")
            return responses

        for page in pages:
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
            }
            for item_type in args.types:
                params[item_type] = "1"

            data = self._request_json_http(
                f"{AMIAMI_API_ROOT}/items",
                params,
                extra_headers=extra_headers,
                cookies=cookies,
            )
            responses[page] = AmiAmiItemsResponse(**data)
            print(f"Fetched page {page}")

        return responses

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
        params = {code_type: code}

        # Crawl details page for given item
        url = f"{AMIAMI_API_ROOT}/item"
        if browser_client is not None:
            normalized_params = {
                key: str(getattr(value, "value", value)) for key, value in params.items()
            }
            data = self._with_retry(
                lambda: browser_client.get_json(url, normalized_params, self.headers),
                context=f"{url} {code}",
            )
        else:
            data = self._request_json(url, params)

        sleep(self.scrap_sleep_time)
        return AmiAmiItemResponse(**data)

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
    ) -> List[AmiAmiItemOutput]:
        if self.detail_enrichment_disabled:
            mapped_items = []
        else:
            try:
                if self.transport == "browser" and cookies is not None:
                    with BrowserJsonClient(
                        start_url=AMIAMI_START_URL,
                        browser_channel=AMIAMI_BROWSER_CHANNEL,
                        headless=AMIAMI_HEADLESS,
                        initial_cookies=cookies,
                    ) as client:
                        mapped_items = self._scrap_item(
                            item.gcode,
                            "gcode",
                            browser_client=client,
                        )
                else:
                    mapped_items = self._scrap_item(item.gcode, "gcode")
            except Exception as e:
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
        filename = f"{timestamp}-{args.stringify()}.json"
        with open(join(OUTPUT_DIR, filename), "w", encoding="utf-8") as f:
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
        # Open raw data file
        filepath = join(OUTPUT_DIR, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            base_data_parsed = AmiAmiItemsDump(**json_load(f))
        amiami_items = base_data_parsed.items

        # Open output file, if any, and retrieve checkpoint variables
        new_filename = f"{timestamp}-mapped_items.json"
        new_filepath = join(WEB_DATA_DIR, new_filename)
        try:
            with open(new_filepath, "r", encoding="utf-8") as f:
                result_data_parsed = AmiAmiItemOutputDump(**json_load(f))
            start_index = result_data_parsed.current_index
            result_mapped: List[AmiAmiItemOutput] = result_data_parsed.items
        except FileNotFoundError:
            start_index = -1
            result_mapped: List[AmiAmiItemOutput] = []
        else:
            print("> Data retrieved from file")

        def save_checkpoint(current_index: int):
            with open(new_filepath, "w", encoding="utf-8") as f:
                save_model_to_json(
                    f,
                    AmiAmiItemOutputDump(
                        current_index=current_index,
                        items_length=len(result_mapped),
                        items=result_mapped,
                    ),
                )

        cookies: Optional[List[Dict]] = None
        if self.transport == "browser" and self.browser_client is not None:
            cookies = self.browser_client.export_cookies()

        # Loop over items to scrap their details pages (start at next item from checkpoint)
        for batch_start in range(start_index + 1, len(amiami_items), self.detail_workers):
            batch_items = amiami_items[batch_start: batch_start + self.detail_workers]
            batch_results: Dict[int, List[AmiAmiItemOutput]] = {}

            detail_jobs = []
            for offset, item in enumerate(batch_items):
                index = batch_start + offset
                print(
                    f"({index + 1}/{len(amiami_items)}) On item {item.gcode}",
                    f"https://www.amiami.com/eng/detail/?gcode={item.gcode}",
                )
                if (
                    not self.detail_enrichment_disabled
                    and ((item.is_preowned and self.fetch_preowned_details) or self.always_scrap_details)
                ):
                    detail_jobs.append((index, item))
                else:
                    batch_results[index] = [item.minify()]

            if detail_jobs:
                if self.detail_workers > 1:
                    with ThreadPoolExecutor(max_workers=self.detail_workers) as executor:
                        futures = {
                            executor.submit(self._enrich_item_with_details, item, cookies): index
                            for index, item in detail_jobs
                        }
                        for future in as_completed(futures):
                            index = futures[future]
                            batch_results[index] = future.result()
                else:
                    for index, item in detail_jobs:
                        batch_results[index] = self._enrich_item_with_details(item, cookies)

            for offset, _item in enumerate(batch_items):
                index = batch_start + offset
                result_mapped.extend(batch_results[index])
                if (index + 1) % self.enrich_save_every == 0 or index == len(amiami_items) - 1:
                    print(f"Saving checkpoint at item {index + 1}...\n")
                    save_checkpoint(index)

        if len(amiami_items) == 0:
            save_checkpoint(start_index)

        # Save final filepath (if not there yet)
        if exists(DATA_LIST_FILE):
            with open(DATA_LIST_FILE, "r") as f:
                existing_files = set(f.read().splitlines())
        else:
            existing_files = set()

        with open(DATA_LIST_FILE, "a") as f:
            if new_filename not in existing_files:
                f.write(new_filename + "\n")
