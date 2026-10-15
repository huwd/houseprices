"""Build-time site configuration shared by build_page.py and build_msoa_page.py."""

import os
import pathlib

from dotenv import load_dotenv

CARTO_KEY_PLACEHOLDER = "__CARTO_API_KEY__"


def render_shared_js(source: pathlib.Path, dest: pathlib.Path) -> None:
    """Copy shared.js to dest, injecting CARTO_API_KEY (from the env or .env).

    Raises SystemExit if the key is unset, so a keyless build never ships.
    """
    load_dotenv()
    key = os.environ.get("CARTO_API_KEY", "").strip()
    if not key:
        raise SystemExit(
            "CARTO_API_KEY is not set. Add it to .env (see .env.example) "
            "or export it in the environment."
        )
    dest.write_text(source.read_text().replace(CARTO_KEY_PLACEHOLDER, key))


if __name__ == "__main__":
    # Deploy-time entry point: bake the key into the already-built output/.
    scripts = pathlib.Path(__file__).parent
    render_shared_js(scripts / "shared.js", scripts.parent / "output" / "shared.js")
    print("Injected CARTO_API_KEY into output/shared.js")
