from datetime import datetime
from random import randint
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

    def __init__(self, logger, throttle_seconds=10):
        self.log = logger
        self.throttle_seconds = throttle_seconds
        self.last_fetch = 0

    def fetch(self, url: str) -> str:
        """Return the HTML contained in HTTP response."""
        self.throttle()
        self.log.debug(f"Attempting to fetch {url}.")
        resp = requests.get(url, headers=HEADERS[0])
        self.update_last_fetch()
        if resp.url != url:
            self.log.critical("Data label integrity threatened by unexpected redirection detected. Aborting.")
            raise RuntimeError
        self.log.debug(f"HTTP status code {resp.status_code}")
        if not resp.status_code == 200:
            if str(resp.status_code).startswith("5"):
                self.log.debug("5XX status encountered. Retrying.")
                self.throttle()
                return self.fetch(url)
            return None
        return resp.content.decode()

    def throttle(self) -> None:
        """Restrict HTTP GETs to no more than once per throttle_seconds."""
        self.log.debug("Throttling...")
        delta = int(datetime.now().timestamp()) - self.last_fetch
        if delta < self.throttle_seconds:
            interval = self.additive_fuzz(self.throttle_seconds, 0, 3)
            sleep(int(self.throttle_seconds - delta))

    def additive_fuzz(self, value: int, min: int, max: int) -> float:
        fuzz = randint(min*1000,max*1000)/1000
        return value + fuzz

    def update_last_fetch(self) -> None:
        self.last_fetch = int(datetime.now().timestamp())