from os import environ, getcwd, makedirs
from os.path import join
from typing import Literal

# Type aliases

# Possible code types
AmiAmiCodeTypeLiteral = Literal["gcode", "scode"]


# Directories

OUTPUT_DIR = join(getcwd(), "output")
makedirs(OUTPUT_DIR, exist_ok=True)

WEB_DIR = join(getcwd(), "web")
makedirs(WEB_DIR, exist_ok=True)

WEB_DATA_DIR = join(WEB_DIR, "data")
makedirs(WEB_DATA_DIR, exist_ok=True)


# Files

DATA_LIST_FILE = join(WEB_DATA_DIR, "_data_files.txt")


# Env variables

AMIAMI_USER_KEY = environ["AMIAMI_USER_KEY"]
AMIAMI_USER_AGENT = environ["AMIAMI_USER_AGENT"]

AMIAMI_API_ROOT = environ["AMIAMI_API_ROOT"]
AMIAMI_IMG_ROOT = environ["AMIAMI_IMG_ROOT"]

ITEMS_PER_PAGE = int(environ["ITEMS_PER_PAGE"])

BROWSER = environ["BROWSER"]
AMIAMI_TRANSPORT = environ.get("AMIAMI_TRANSPORT", "browser")
AMIAMI_BROWSER_CHANNEL = environ.get("AMIAMI_BROWSER_CHANNEL", "chrome")
AMIAMI_HEADLESS = environ.get("AMIAMI_HEADLESS", "false").lower() == "true"
AMIAMI_START_URL = environ.get("AMIAMI_START_URL", "https://www.amiami.com/eng/")
AMIAMI_CRAWL_SLEEP_SECONDS = float(environ.get("AMIAMI_CRAWL_SLEEP_SECONDS", "1"))
AMIAMI_DETAIL_SLEEP_SECONDS = float(environ.get("AMIAMI_DETAIL_SLEEP_SECONDS", "1.5"))
AMIAMI_FETCH_PREOWNED_DETAILS = (
    environ.get("AMIAMI_FETCH_PREOWNED_DETAILS", "true").lower() == "true"
)
AMIAMI_PAGE_WORKERS = int(environ.get("AMIAMI_PAGE_WORKERS", "1"))
AMIAMI_DETAIL_WORKERS = int(environ.get("AMIAMI_DETAIL_WORKERS", "1"))
AMIAMI_MAX_RETRIES = int(environ.get("AMIAMI_MAX_RETRIES", "5"))
AMIAMI_RETRY_BASE_SECONDS = float(environ.get("AMIAMI_RETRY_BASE_SECONDS", "5"))
AMIAMI_ENRICH_SAVE_EVERY = int(environ.get("AMIAMI_ENRICH_SAVE_EVERY", "100"))
