from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from curl_cffi import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_DIR = REPO_ROOT / "core"
ENV_FILE = REPO_ROOT / ".env"
ENV_DEFAULT_FILE = REPO_ROOT / ".env.default"
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))

from config import (  # noqa: E402
    AMIAMI_BROWSER_CHANNEL,
    AMIAMI_HEADLESS,
    AMIAMI_START_URL,
    AMIAMI_USER_AGENT,
    BROWSER,
    OUTPUT_DIR,
)
from models.amiami.enums import (  # noqa: E402
    ItemCategory1Enum,
    ItemCategory2Enum,
    ItemCategory3Enum,
    ItemSortingEnum,
    ItemTypeEnum,
)
from scrapers.browser_client import BrowserJsonClient  # noqa: E402


INTERESTING_SELECT_NAMES = {"category", "s_cate1", "s_cate2", "s_cate3", "s_sortkey"}
INTERESTING_INPUT_NAMES = {
    "s_st_list_preorder_available",
    "s_st_list_backorder_available",
    "s_st_list_newitem_available",
    "s_st_condition_flg",
    "s_st_list_store_bonus",
    "s_st_saleitem",
}


@dataclass
class PendingInput:
    name: str
    value: str
    input_id: Optional[str]
    checked: bool
    label: Optional[str] = None


@dataclass
class LabelContext:
    for_id: Optional[str]
    text_parts: List[str] = field(default_factory=list)
    input_keys: List[str] = field(default_factory=list)


class AmiAmiSearchFilterParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.select_options: Dict[str, List[Dict[str, str]]] = {
            name: [] for name in INTERESTING_SELECT_NAMES
        }
        self.checkbox_options: Dict[str, List[Dict[str, str]]] = {}
        self.link_options: Dict[str, List[Dict[str, str]]] = {}
        self._current_select_name: Optional[str] = None
        self._current_option_value: Optional[str] = None
        self._current_option_text: List[str] = []
        self._current_link_filter_name: Optional[str] = None
        self._current_link_filter_text: List[str] = []
        self._labels_stack: List[LabelContext] = []
        self._inputs_by_id: Dict[str, PendingInput] = {}
        self._all_inputs: List[PendingInput] = []

    def handle_starttag(self, tag: str, attrs):
        attrs_dict = dict(attrs)

        if tag == "select":
            select_name = attrs_dict.get("name")
            self._current_select_name = select_name if select_name in INTERESTING_SELECT_NAMES else None
            return

        if tag == "option" and self._current_select_name is not None:
            self._current_option_value = attrs_dict.get("value", "")
            self._current_option_text = []
            return

        if tag == "label":
            self._labels_stack.append(LabelContext(for_id=attrs_dict.get("for")))
            return

        if tag == "a":
            data_value = attrs_dict.get("data-value")
            if data_value in INTERESTING_INPUT_NAMES:
                self._current_link_filter_name = data_value
                self._current_link_filter_text = []
            return

        if tag != "input":
            return

        input_type = (attrs_dict.get("type") or "").lower()
        input_name = attrs_dict.get("name") or ""
        if input_type != "checkbox" or input_name not in INTERESTING_INPUT_NAMES:
            return

        pending = PendingInput(
            name=input_name,
            value=attrs_dict.get("value", "1"),
            input_id=attrs_dict.get("id"),
            checked="checked" in attrs_dict,
        )
        self._all_inputs.append(pending)
        if pending.input_id:
            self._inputs_by_id[pending.input_id] = pending

        if self._labels_stack:
            self._labels_stack[-1].input_keys.append(self._pending_key(pending))

    def handle_endtag(self, tag: str):
        if tag == "option" and self._current_select_name is not None and self._current_option_value is not None:
            text = " ".join(part.strip() for part in self._current_option_text if part.strip()).strip()
            if self._current_option_value:
                self.select_options[self._current_select_name].append(
                    {"value": self._current_option_value, "label": text or self._current_option_value}
                )
            self._current_option_value = None
            self._current_option_text = []
            return

        if tag == "select":
            self._current_select_name = None
            return

        if tag == "a" and self._current_link_filter_name is not None:
            text = " ".join(part.strip() for part in self._current_link_filter_text if part.strip()).strip()
            if text:
                existing = self.link_options.setdefault(self._current_link_filter_name, [])
                if not any(item["label"] == text for item in existing):
                    existing.append({"value": "1", "label": text, "checked": False})
            self._current_link_filter_name = None
            self._current_link_filter_text = []
            return

        if tag == "label" and self._labels_stack:
            label_context = self._labels_stack.pop()
            label_text = " ".join(part.strip() for part in label_context.text_parts if part.strip()).strip()

            if label_context.for_id and label_context.for_id in self._inputs_by_id:
                self._inputs_by_id[label_context.for_id].label = label_text

            for pending_key in label_context.input_keys:
                pending = next(
                    (item for item in self._all_inputs if self._pending_key(item) == pending_key),
                    None,
                )
                if pending is not None:
                    pending.label = label_text

    def handle_data(self, data: str):
        if self._current_select_name is not None and self._current_option_value is not None:
            self._current_option_text.append(data)

        if self._current_link_filter_name is not None:
            self._current_link_filter_text.append(data)

        if self._labels_stack:
            self._labels_stack[-1].text_parts.append(data)

    def finalize(self) -> Dict[str, List[Dict[str, str]]]:
        for pending in self._all_inputs:
            self.checkbox_options.setdefault(pending.name, []).append(
                {
                    "value": pending.value,
                    "label": pending.label or pending.value,
                    "checked": pending.checked,
                }
            )

        merged_checkbox_options: Dict[str, List[Dict[str, str]]] = {}
        for name in sorted(set(self.checkbox_options) | set(self.link_options)):
            merged_items = []
            for source in (self.checkbox_options.get(name, []), self.link_options.get(name, [])):
                for item in source:
                    if not any(existing["label"] == item["label"] and existing["value"] == item["value"] for existing in merged_items):
                        merged_items.append(item)
            if merged_items:
                merged_checkbox_options[name] = merged_items

        return {
            "selects": {name: options for name, options in self.select_options.items() if options},
            "checkboxes": merged_checkbox_options,
        }

    @staticmethod
    def _pending_key(pending: PendingInput) -> str:
        return f"{pending.name}|{pending.value}|{pending.input_id or ''}"


def enum_to_options(enum_cls) -> List[Dict[str, str]]:
    return [
        {"name": member.name, "value": member.value}
        for member in enum_cls
    ]


def build_enum_snapshot() -> Dict[str, List[Dict[str, str]]]:
    return {
        "sort": enum_to_options(ItemSortingEnum),
        "types": enum_to_options(ItemTypeEnum),
        "category1": enum_to_options(ItemCategory1Enum),
        "category2": enum_to_options(ItemCategory2Enum),
        "category3": enum_to_options(ItemCategory3Enum),
    }


def _set_env_value(env_path: Path, key: str, value: str):
    lines: List[str] = []
    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines()

    replacement = f'{key} = "{value}"'
    for index, line in enumerate(lines):
        if line.strip().startswith(f"{key} " ) or line.strip().startswith(f"{key}="):
            lines[index] = replacement
            break
    else:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(replacement)

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _names_by_discovered_values(
    enum_options: List[Dict[str, str]],
    discovered_values: Optional[set[str]],
) -> List[str]:
    if not discovered_values:
        return [option["name"] for option in enum_options]
    return [
        option["name"] for option in enum_options if option["value"] in discovered_values
    ] or [option["name"] for option in enum_options]


def _names_by_discovered_keys(
    enum_options: List[Dict[str, str]],
    discovered_keys: Optional[set[str]],
) -> List[str]:
    if not discovered_keys:
        return [option["name"] for option in enum_options]
    return [
        option["name"] for option in enum_options if option["value"] in discovered_keys
    ] or [option["name"] for option in enum_options]


def update_env_available_options(discovered: Dict[str, object]):
    enum_snapshot = build_enum_snapshot()
    discovered_data = discovered["data"]
    discovered_category_values = {
        option["value"] for option in discovered_data.get("selects", {}).get("category", [])
    }
    discovered_type_keys = set(discovered_data.get("checkboxes", {}).keys())

    available_env_values = {
        "AMIAMI_AVAILABLE_SCRAPE_TYPES": ",".join(
            _names_by_discovered_keys(enum_snapshot["types"], discovered_type_keys)
        ),
        "AMIAMI_AVAILABLE_SCRAPE_CATEGORY1": ",".join(
            _names_by_discovered_values(enum_snapshot["category1"], discovered_category_values)
        ),
        "AMIAMI_AVAILABLE_SCRAPE_CATEGORY2": ",".join(
            _names_by_discovered_values(enum_snapshot["category2"], discovered_category_values)
        ),
        "AMIAMI_AVAILABLE_SCRAPE_CATEGORY3": ",".join(
            _names_by_discovered_values(enum_snapshot["category3"], discovered_category_values)
        ),
        "AMIAMI_AVAILABLE_SCRAPE_SORT_KEY": ",".join(
            option["name"] for option in enum_snapshot["sort"]
        ),
    }

    for key, value in available_env_values.items():
        _set_env_value(ENV_FILE, key, value)

    update_env_file_available_comments(ENV_FILE, available_env_values)
    update_env_file_available_comments(ENV_DEFAULT_FILE, available_env_values)


def _replace_available_values_block(
    lines: List[str],
    setting_key: str,
    values: List[str],
) -> List[str]:
    try:
        setting_index = next(
            index for index, line in enumerate(lines) if line.strip().startswith(f"{setting_key} ")
        )
    except StopIteration:
        return lines

    available_header_index = None
    for index in range(setting_index - 1, -1, -1):
        stripped = lines[index].strip()
        if stripped.startswith("AMIAMI_"):
            break
        if stripped == "# Available values:":
            available_header_index = index
            break

    if available_header_index is None:
        return lines

    block_end = available_header_index + 1
    while block_end < setting_index and lines[block_end].strip().startswith("# - "):
        block_end += 1

    replacement = ["# Available values:"] + [f"# - {value}" for value in values]
    return lines[:available_header_index] + replacement + lines[block_end:]


def update_env_file_available_comments(env_path: Path, available_env_values: Dict[str, str]):
    if not env_path.exists():
        return

    lines = env_path.read_text(encoding="utf-8").splitlines()
    comment_mappings = [
        ("AMIAMI_SCRAPE_TYPES", "AMIAMI_AVAILABLE_SCRAPE_TYPES"),
        ("AMIAMI_SCRAPE_CATEGORY1", "AMIAMI_AVAILABLE_SCRAPE_CATEGORY1"),
        ("AMIAMI_SCRAPE_CATEGORY2", "AMIAMI_AVAILABLE_SCRAPE_CATEGORY2"),
        ("AMIAMI_SCRAPE_CATEGORY3", "AMIAMI_AVAILABLE_SCRAPE_CATEGORY3"),
        ("AMIAMI_SCRAPE_SORT_KEY", "AMIAMI_AVAILABLE_SCRAPE_SORT_KEY"),
    ]

    for setting_key, available_key in comment_mappings:
        values = [value for value in available_env_values.get(available_key, "").split(",") if value]
        if values:
            lines = _replace_available_values_block(lines, setting_key, values)

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def fetch_html(url: str) -> str:
    proxy_keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]
    saved_proxy_values = {key: os.environ.get(key) for key in proxy_keys}

    try:
        for key in proxy_keys:
            os.environ.pop(key, None)

        response = requests.get(
            url,
            headers={"User-Agent": AMIAMI_USER_AGENT},
            impersonate=BROWSER,
        )
        response.raise_for_status()
        return response.text
    finally:
        for key, value in saved_proxy_values.items():
            if value is not None:
                os.environ[key] = value


def fetch_html_browser(url: str) -> str:
    with BrowserJsonClient(
        start_url=AMIAMI_START_URL,
        browser_channel=AMIAMI_BROWSER_CHANNEL,
        headless=AMIAMI_HEADLESS,
    ) as client:
        return client.get_html(url, headers={"User-Agent": AMIAMI_USER_AGENT})


def parse_discovery_html(html: str) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    parser = AmiAmiSearchFilterParser()
    parser.feed(html)
    return parser.finalize()


def count_discovered_options(parsed: Dict[str, Dict[str, List[Dict[str, str]]]]) -> int:
    return sum(len(options) for options in parsed["selects"].values()) + sum(
        len(options) for options in parsed["checkboxes"].values()
    )


def save_debug_html(method: str, url: str, html: str):
    debug_dir = Path(OUTPUT_DIR) / "discovery-html"
    debug_dir.mkdir(parents=True, exist_ok=True)

    parsed_url = urlparse(url)
    path_part = parsed_url.path.strip("/").replace("/", "_") or "root"
    query_part = (parsed_url.query or "no-query").replace("&", "_").replace("=", "-")
    filename = f"{method}-{path_part}-{query_part}.html"
    debug_path = debug_dir / filename
    debug_path.write_text(html, encoding="utf-8")


def discover_from_urls(urls: List[str]) -> Dict[str, object]:
    attempts: List[Dict[str, str]] = []
    for url in urls:
        try:
            html = fetch_html(url)
            save_debug_html("direct", url, html)
            parsed = parse_discovery_html(html)
            discovered_count = count_discovered_options(parsed)
            attempts.append(
                {
                    "url": url,
                    "method": "direct",
                    "status": "ok",
                    "discovered_count": str(discovered_count),
                }
            )
            if discovered_count > 0:
                return {"source_url": url, "data": parsed, "attempts": attempts}
        except Exception as exc:
            attempts.append({"url": url, "method": "direct", "status": "error", "error": str(exc)})

        try:
            html = fetch_html_browser(url)
            save_debug_html("browser", url, html)
            parsed = parse_discovery_html(html)
            discovered_count = count_discovered_options(parsed)
            attempts.append(
                {
                    "url": url,
                    "method": "browser",
                    "status": "ok",
                    "discovered_count": str(discovered_count),
                }
            )
            if discovered_count > 0:
                return {"source_url": url, "data": parsed, "attempts": attempts}
        except Exception as exc:
            attempts.append({"url": url, "method": "browser", "status": "error", "error": str(exc)})

    return {"source_url": None, "data": {"selects": {}, "checkboxes": {}}, "attempts": attempts}


def build_default_urls() -> List[str]:
    return [
        "https://www.amiami.com/eng/search/list/?s_keywords=",
        "https://www.amiami.com/eng/search/list/",
        AMIAMI_START_URL,
    ]


def build_output(args, discovered: Dict[str, object]) -> Dict[str, object]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "enum_snapshot": build_enum_snapshot(),
        "discovered_from_site": discovered["data"],
        "source_url": discovered["source_url"],
        "attempts": discovered["attempts"],
        "notes": [
            "enum_snapshot comes from the local codebase and is always available",
            "discovered_from_site is best-effort and depends on AmiAmi page structure and network accessibility",
            "the script tries direct HTTP first and then a browser-backed fetch for each candidate URL",
        ],
        "requested_urls": args.urls,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Discover AmiAmi search options from the local enums and, when possible, from the live site.",
    )
    parser.add_argument(
        "--output",
        default=str(Path(OUTPUT_DIR) / "amiami-discovery.json"),
        help="Path to the JSON output file.",
    )
    parser.add_argument(
        "--url",
        dest="urls",
        action="append",
        help="Candidate URL to inspect. Can be passed multiple times.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.urls:
        args.urls = build_default_urls()

    discovered = discover_from_urls(args.urls)
    update_env_available_options(discovered)
    output = build_output(args, discovered)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Discovery JSON saved to '{output_path}'")
    if discovered["source_url"]:
        print(f"Live options were discovered from: {discovered['source_url']}")
    else:
        print("Live site discovery did not find structured options. The JSON still contains the local enum snapshot.")


if __name__ == "__main__":
    main()
