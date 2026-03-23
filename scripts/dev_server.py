#!/usr/bin/env python3
"""Dev server: watches scripts/ and live-reloads the browser on rebuild.

Usage:
    uv run scripts/dev_server.py
    make dev
"""

import pathlib
import subprocess

import livereload

ROOT = pathlib.Path(__file__).parent.parent


def rebuild():
    subprocess.run(["make", "page"], cwd=ROOT, check=False)


server = livereload.Server()
server.watch(str(ROOT / "scripts" / "page.js"), rebuild)
server.watch(str(ROOT / "scripts" / "page.css"), rebuild)
server.watch(str(ROOT / "scripts" / "page_template.html"), rebuild)
server.watch(str(ROOT / "scripts" / "build_page.py"), rebuild)
server.serve(root=str(ROOT / "output"), port=8000, host="localhost")
