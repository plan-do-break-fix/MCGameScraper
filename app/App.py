import logging
from os import environ
from typing import List

from bs4 import BeautifulSoup

from app import parse
from app import SqliteInterface
from app import WebInterface



class App:

    def __init__(self, data_path="."):
        self.log = self.get_logger("MCGameScraper")
        self.db = SqliteInterface.Interface(data_path=data_path)
        self.log.debug("Database connection extablished.")
        self.web = WebInterface.Interface(self.get_logger("WebInterface"))
        self.log.debug("Web interface connection extablished.")

    def get_logger(self, name):
        logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(name)-14s] %(levelname)-8s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger

    def main(self):
        self.populate_title_by_genre_urls_to_crawl()
        self.log.debug("All title-by-genre listing page URLs present in database.")
        run = True
        while run:
            final_page_crawl = self.db.get_one_incomplete_last_page_crawl()
            if not final_page_crawl:
                run = False

            else:
                self.populate_titles_by_genre_final_page_number(final_page_crawl)
        self.log.debug("All title-by-genre listing page final page numbers recorded.")
        run = True
        while run:
            title_crawl = self.db.get_one_incomplete_title_crawl()
            if not title_crawl:
                run = False
            else:
                self.populate_games(*title_crawl)
        while run:
            game_crawl = self.db.get_one_incomplete_game_crawl()
            if not game_crawl:
                return True
            crawl_type = game_crawl.pop()
            if crawl_type == "user":
                self.scrape_user_reviews(*crawl)
            elif crawl_type == "critic":
                self.scrape_critic_reviews(*crawl)

    # Main function methods
    def populate_title_by_genre_urls_to_crawl(self) -> bool:
        self.log.info("Parsing title-by-genre listing page URLs.")
        for slug in self.db.get_platforms_without_genre_crawl_urls():
            platform_home_url = f"https://www.metacritic.com/game/{slug}"
            html = self.web.fetch(platform_home_url)
            soup = BeautifulSoup(html, features="html5lib")
            for url in parse.get_title_by_genre_listing_page_urls(soup):
                if not self.db.platform_genre_crawl_url_exists(url):
                    genre = url.split("/")[-2]
                    self.db.new_platform_genre_crawl_url(slug, genre, url)
                    self.log.debug("New titles-by-genre listing page URL recorded.")
        return True

    def populate_titles_by_genre_final_page_number(self, url) -> bool:
        self.log.info("Parsing title-by-genre listing final page number.")
        html = self.web.fetch(url)
        soup = BeautifulSoup(html, features="html5lib")
        final_page = int(parse.get_last_page_number(soup))
        self.db.add_final_platform_genre_page_number(url, final_page)
        self.log.debug(f"{url} has {final_page} pages.")
        return True

    def populate_games(self, url, page) -> bool:
        self.log.info("Scraping games from title-by-genre listing page.")
        platform_pk = self.db.get_platform_pk_from_genre_crawl_url(url)
        url_pg = f"{url}?page={page}"
        html = self.web.fetch(url_pg)
        soup = BeautifulSoup(html, features="html5lib")
        for game in parse.scrape_games(soup):
            game["platform"] = platform_pk
            game_pk = self.db.new_game(**game)
            self.log.debug(f"{game['title']} added to games.")
        self.db.update_last_platform_genre_page_scraped(url, page)
        self.log.debug("All games scraped from page.")
        return True

    def scrape_user_reviews(self, game_pk, game_slug, platform_pk, platform_slug, page_number) -> bool:
            url = self.review_page_url("user", game_slug, platform_slug, page_number)
            self.log.info(f"Scraping reviews from {url}.")
            html = self.web.fetch(url)
            soup = BeautifulSoup(html, features="html5lib")
            if page_number == 0:  # final page number is Null
                final_page_number = parse.get_last_page_number(soup) 
                self.db.add_final_review_page_number(game_pk, "user", final_page_number)
            for review in parse.scrape_user_reviews(soup):
                review["game_pk"] = game_pk
                self.db.new_user_review(**review)
                self.log.debug("New user review added to user_reviews.")
            self.db.update_last_review_page_scraped(game_pk, "user", page_number)
            self.log.debug("All reviews scraped from page.")
            return True

    def scrape_critic_reviews(self, game_pk, game_slug, platform_pk, platform_slug, page_number) -> bool:
            self.log.info(f"Scraping reviews from {url}.")
            url = self.review_page_url("critic", game_slug, platform_slug, page_number)
            html = self.web.fetch(url)
            soup = BeautifulSoup(html, features="html5lib")
            if page_number == 0:  # final page number is Null
                final_page_number = parse.get_last_page_number(soup) 
                self.db.add_final_review_page_number(game_pk, "critic", final_page_number)
            for review in parse.scrape_critic_reviews(soup):
                review["game_pk"] = game_pk
                self.db.new_critic_review(**review)
                self.log.debug("New critic review added to critic_reviews.")
            self.db.update_last_review_page_scraped(game_pk, "critic", page_number)
            self.log.debug("All reviews scraped from page.")
            return True
    
    # URL constructors
    def game_page_url(self, game_slug, platform_slug):
        return f"https://www.metacritic.com/game/{platform_slug}/{game_slug}"

    def review_page_url(self, review_type, game_slug, platform_slug, page=0):
        if review_type not in ["critic", "user"]:
            raise RuntimeError
        return f"{self.game_page_url(game_slug, platform_slug)}/{review_type}-reviews?page={page}"


if __name__ == "__main__":
    app = App()
    app.main()
