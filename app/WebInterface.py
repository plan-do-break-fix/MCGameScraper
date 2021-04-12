from datetime import datetime
import requests
from time import sleep


HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36",
     "Upgrade-Insecure-Requests": "1",
     "DNT": "1",
     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
     "Accept-Language": "en-US,en;q=0.5",
     "Accept-Encoding": "gzip, deflate"}
]


class Interface:

    def __init__(self, throttle_seconds=10):
        self.throttle_seconds = throttle_seconds
        self.last_fetch = 0

    def fetch(self, url: str) -> str:
        """Return the HTML contained in HTTP response."""
        self.throttle()
        resp = requests.get(url, headers=HEADERS[0])
        self.update_last_fetch()
        if not resp.status_code == 200:
            return None
        return resp.content

    def throttle(self) -> None:
        """Restrict HTTP GETs to no more than once per throttle_seconds."""
        delta = int(datetime.now().timestamp()) - self.last_fetch
        if delta < self.throttle_seconds:
            sleep(int(self.last_fetch - delta))

    def update_last_fetch(self) -> None:
        self.last_fetch = int(datetime.now().timestamp())
