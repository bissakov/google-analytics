import logging
import os
import time
import urllib.parse
from typing import Callable, Dict, List, Optional, Tuple

import requests
import requests.adapters


def _send_message(
    token: str,
    chat_id: str,
    message: Optional[str] = "...",
) -> bool:
    api_url = "https://api.telegram.org/bot{token}/"
    api_url = api_url.format(token=token)
    send_data: Dict[str, Optional[str]] = {"chat_id": chat_id}
    files = None

    url = urllib.parse.urljoin(api_url, "sendMessage")
    send_data["text"] = message

    response = requests.post(url, data=send_data, files=files)

    method = url.split("/")[-1]
    data = "" if not hasattr(response, "json") else response.json()
    logging.info(
        f"Response for '{method}': {response}\n"
        f"Is 200: {response.status_code == 200}\n"
        f"Data: {data}"
    )
    response.raise_for_status()
    return response.status_code == 200


def get_secrets() -> Tuple[List[str], str]:
    chat_id = os.getenv("CHAT_ID")
    assert chat_id is not None, "Environment variable 'CHAT_ID' is not set."
    tokens_str = os.getenv("TOKENS")
    assert tokens_str is not None, "Environment variable 'TOKENS' is not set."

    if tokens_str is None:
        raise EnvironmentError("Environment variable 'TOKENS' is not set.")
    tokens: List[str] = tokens_str.split(";")

    return tokens, chat_id


def send_with_retry(
    send_func: Callable[[str, str, Optional[str]], bool],
    tokens: List[str],
    chat_id: str,
    message: Optional[str] = "...",
) -> bool:
    retry = 0
    while retry < 10:
        try:
            token = tokens[retry % len(tokens)]
            send_func(token, chat_id, message)
            return True
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
            requests.exceptions.HTTPError,
        ) as e:
            logging.exception(e)
            logging.warning(f"{e} intercepted. Retry {retry + 1}/10")
            retry += 1
            time.sleep(5)

    if retry == 10:
        logging.error("Failed to send message.")
        raise ConnectionError("Failed to send message.")
    return False


def send_message(
    message: Optional[str] = "...",
) -> None:
    tokens, chat_id = get_secrets()
    send_with_retry(_send_message, tokens, chat_id, message)
