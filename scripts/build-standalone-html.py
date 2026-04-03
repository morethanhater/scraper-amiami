from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: build-standalone-html.py <mapped-json> <output-html>")
        return 1

    repo_root = Path(__file__).resolve().parent.parent
    mapped_json_path = Path(sys.argv[1]).resolve()
    output_html_path = Path(sys.argv[2]).resolve()

    index_html = (repo_root / "web" / "index.html").read_text(encoding="utf-8")
    styles_css = (repo_root / "web" / "styles.css").read_text(encoding="utf-8")
    script_js = (repo_root / "web" / "script.js").read_text(encoding="utf-8")

    mapped_data = json.loads(mapped_json_path.read_text(encoding="utf-8"))
    embedded_items = mapped_data.get("items", [])
    embedded_json = json.dumps(embedded_items, ensure_ascii=False).replace("</script", "<\\/script")

    standalone_html = index_html.replace(
        '<link rel="stylesheet" href="styles.css">',
        f"<style>\n{styles_css}\n</style>",
    ).replace(
        '<script src="script.js"></script>',
        "<script>\n"
        f"window.__AMIAMI_EMBEDDED_DATA__ = {embedded_json};\n"
        f"window.__AMIAMI_EMBEDDED_SOURCE__ = {json.dumps(mapped_json_path.name)};\n"
        "</script>\n"
        f"<script>\n{script_js}\n</script>",
    )

    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    output_html_path.write_text(standalone_html, encoding="utf-8")
    print(f"Created standalone HTML: {output_html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
