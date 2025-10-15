import os
import random
import requests
from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI(title="Movies API Proxy", version="1.3.0")

GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION')
MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT'))
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL').rstrip("/")
MONOLITH_URL = os.getenv('MONOLITH_URL').rstrip("/")


def choose_backend(path: str):
    if path == "movies" and GRADUAL_MIGRATION:
        roll = random.randint(0, 99)  # 0..99
        if roll < MOVIES_MIGRATION_PERCENT:
            return MOVIES_SERVICE_URL
    return MONOLITH_URL


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/{path}")
def proxy_api(path: str, request: Request):
    base = choose_backend(path)

    target_url = f"{base}/api/{path}"

    outbound_headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}

    try:
        upstream = requests.get(target_url, headers=outbound_headers)
    except requests.RequestException:
        return Response(content=b"Bad Gateway", status_code=502)

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=dict(upstream.headers),  # passthrough
        media_type=upstream.headers.get("content-type"),
    )
