from os import environ
from typing import List, Optional, Type, TypeVar

from models.amiami.enums import (
    ItemCategory1Enum,
    ItemCategory2Enum,
    ItemCategory3Enum,
    ItemSortingEnum,
    ItemTypeEnum,
)
from models.amiami.utils import AmiAmiQueryArgs
from scrapers.amiami import AmiAmiScraper

EnumT = TypeVar("EnumT")


def _read_env(name: str, default: str = "") -> str:
    return environ.get(name, default).strip()


def _parse_optional_int(name: str, default: str = "") -> Optional[int]:
    raw = _read_env(name, default)
    if not raw:
        return None
    return int(raw)


def _parse_optional_enum(name: str, enum_cls: Type[EnumT], default: str = "") -> Optional[EnumT]:
    raw = _read_env(name, default)
    if not raw:
        return None

    try:
        return enum_cls[raw.upper()]
    except KeyError:
        try:
            return enum_cls(raw)
        except ValueError as exc:
            valid_names = ", ".join(member.name for member in enum_cls)
            raise ValueError(
                f"Invalid value '{raw}' for {name}. Use one of: {valid_names}"
            ) from exc


def _parse_enum_list(name: str, enum_cls: Type[EnumT], default: str = "") -> List[EnumT]:
    raw = _read_env(name, default)
    values = [item.strip() for item in raw.split(",") if item.strip()]
    parsed: List[EnumT] = []
    invalid_values: List[str] = []

    for value in values:
        try:
            parsed.append(enum_cls[value.upper()])
        except KeyError:
            try:
                parsed.append(enum_cls(value))
            except ValueError:
                invalid_values.append(value)

    if invalid_values:
        valid_names = ", ".join(member.name for member in enum_cls)
        raise ValueError(
            f"Invalid values for {name}: {', '.join(invalid_values)}. Use one of: {valid_names}"
        )

    return parsed


def _build_batch_args() -> List[AmiAmiQueryArgs]:
    return [
        AmiAmiQueryArgs(
            num_pages=_parse_optional_int("AMIAMI_SCRAPE_NUM_PAGES"),
            keyword=_read_env("AMIAMI_SCRAPE_KEYWORD", ""),
            types=_parse_enum_list(
                "AMIAMI_SCRAPE_TYPES",
                ItemTypeEnum,
                "BACK_ORDER,NEW,PRE_ORDER,PRE_OWNED",
            ),
            category1=_parse_optional_enum("AMIAMI_SCRAPE_CATEGORY1", ItemCategory1Enum),
            category2=_parse_optional_enum(
                "AMIAMI_SCRAPE_CATEGORY2",
                ItemCategory2Enum,
                "BISHOUJO_FIGURES",
            ),
            category3=_parse_optional_enum("AMIAMI_SCRAPE_CATEGORY3", ItemCategory3Enum),
            sort_key=_parse_optional_enum(
                "AMIAMI_SCRAPE_SORT_KEY",
                ItemSortingEnum,
                "RECENT_UPDATE",
            ),
        ),
    ]


if __name__ == "__main__":
    print("Init scraper...")
    with AmiAmiScraper(always_scrap_details=False) as amiami:
        batch_args = _build_batch_args()

        print("Starting scraping...")
        for args in batch_args:
            # Scrap website
            timestamp, filename = amiami.run_scraping(args)
            # timestamp, filename = (
            #     "20250318_000540",
            #     "20250318_000540-categories=s_st_condition_flg.json",
            # )

            # Enrich format
            amiami.run_enrich(timestamp, filename)
    print("End scraping")
