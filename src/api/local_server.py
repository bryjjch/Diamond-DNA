"""Local dev server: Flask shim that invokes the Lambda handler.

Lets the React frontend point at a real URL during local development without
deploying to AWS. Each request is converted into an API Gateway HTTP API v2
event, dispatched to ``src.api.handler.handler``, and the Lambda response is
translated back into a Flask response.

Run with::

    python -m src.api.local_server
"""

from __future__ import annotations

import logging
import os

from flask import Flask, Response, request

from .handler import handler as lambda_handler


def create_app() -> Flask:
    app = Flask(__name__)

    @app.route("/", defaults={"path": ""}, methods=["GET", "OPTIONS"])
    @app.route("/<path:path>", methods=["GET", "OPTIONS"])
    def _proxy(path: str) -> Response:
        event = {
            "version": "2.0",
            "rawPath": "/" + path,
            "rawQueryString": request.query_string.decode("utf-8"),
            "queryStringParameters": dict(request.args) or None,
            "headers": {k.lower(): v for k, v in request.headers.items()},
            "requestContext": {
                "http": {
                    "method": request.method,
                    "path": "/" + path,
                }
            },
        }
        result = lambda_handler(event, None)
        return Response(
            result.get("body", ""),
            status=result.get("statusCode", 200),
            headers=result.get("headers", {}),
        )

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    host = os.environ.get("API_HOST", "127.0.0.1")
    port = int(os.environ.get("API_PORT", "5001"))
    app.run(host=host, port=port, debug=os.environ.get("API_DEBUG", "").lower() in ("1", "true", "yes"))


if __name__ == "__main__":
    main()
