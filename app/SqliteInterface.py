import os
from random import choice
import sqlite3


PLATFORMS = [
    "pc",
    "playstation-4",
    "playstation-5",
    "switch",
    "wii-u",
    "xbox-one"
]
GENRES = [
    "action",
    "adventure",
    "fighting",
    "first-person",
    "flight",
    "party",
    "platformer",
    "puzzle",
    "racing",
    "real-time",
    "role-playing",
    "simulation",
    "sports",
    "strategy",
    "third-person",
    "turn-based",
    "wargame",
    "wrestling"
]
SCHEMAS = {
    "platforms": (
        "CREATE TABLE IF NOT EXISTS 'platforms' ("
        "  slug TEXT NOT NULL,"
        "  genre_crawl_complete INTEGER DEFAULT 0"
        ");"
    ),
    "games": (
        "CREATE TABLE IF NOT EXISTS 'games' ("
        "  title TEXT NOT NULL,"
        "  slug TEXT NOT NULL,"
        "  platform INTEGER NOT NULL,"
        "  released TEXT NOT NULL,"
        "  final_user_review_page_number INTEGER DEFAULT NULL,"
        "  last_user_review_page_scraped INTEGER DEFAULT NULL,"
        "  final critic_review_page_number INTEGER DEFAULT NULL,"
        "  last_critic_review_page_scraped INTEGER DEFAULT NULL,"
        "  FOREIGN KEY (platform) REFERENCES platforms (rowid)"
        ");"
    ),
    "critic_reviews": (
        "CREATE TABLE IF NOT EXISTS 'user_reviews' ("
        "  game INTEGER NOT NULL,"
        "  author TEXT NOT NULL,"
        "  date TEXT NOT NULL,"
        "  grade INTEGER NOT NULL,"
        "  body TEXT NOT NULL,"
        "  FOREIGN KEY (game) REFERENCES games (rowid)"
        ");"
    ),
    "user_reviews": (
        "CREATE TABLE IF NOT EXISTS 'user_reviews' ("
        "  game INTEGER NOT NULL,"
        "  review_id INTEGER NOT NULL,"
        "  author TEXT NOT NULL,"
        "  date TEXT NOT NULL,"
        "  grade INTEGER NOT NULL,"
        "  body TEXT NOT NULL,"
        "  votes_total INTEGER NOT NULL,"
        "  votes_helpful INTEGER NOT NULL,"
        "  FOREIGN KEY (game) REFERENCES games (rowid)"
        ");"
    ),
    "platform_genre_crawls": (
        "CREATE TABLE IF NOT EXISTS 'platform_genre_crawls' ("
        "  platform INTEGER NOT NULL,"
        "  genre STRING NOT NULL,"
        "  url STRING DEFAULT NULL,"
        "  final_page_number INTEGER DEFAULT NULL,"
        "  last_page_scraped INTEGER DEFAULT NULL,"
        "  FOREIGN KEY (platform) REFERENCES platforms (rowid)"
        ");"
    )
}


class Interface:

    def __init__(self, data_path: str):
        if not os.path.isfile(f"{data_path}/MCScraper.sqlite3.db"):
            self.make_database(data_path)
        self.conn = sqlite3.connect(f"{data_path}/MCScraper.sqlite3.db")
        self.c = self.conn.cursor()

    # Utility, initial table population, etc.
    def make_database(self, data_path):
        self.conn = sqlite3.connect(f"{data_path}/MCScraper.sqlite3.db")
        self.c = self.conn.cursor()
        for statement in SCHEMAS:
            self.c.execute(statement)
        for platform in PLATFORMS.keys():
            self.c.execute("INSERT INTO platforms (name, slug) VALUES (?,?)",
                      (platform, PLATFORMS["platform"])) 
        for genre in GENRES:
            self.c.execute("INSERT INTO genres (slug) VALUES (?)", (genre,))
        self.conn.commit()

    def get_title_by_genre_crawls_without_final_page(self) -> List:
        self.c.execute("SELECT url FROM platform_genre_crawls WHERE final_page_numer=NULL")
        return self.c.fetchall()

    def get_platform_slug(self, platform_pk) -> str:
        self.c.execute("SELECT slug FROM platforms WHERE rowid=?", (platform_pk,))
        return self.c.fetchone()[0]

    def get_platform_pk_from_genre_crawl_url(self, url):
        self.c.execute("SELECT platform FROM platform_genre_crawls WHERE url=?",
                       (url))
        return self.c.fetchone()[0]

    def get_platforms_without_genre_crawl_urls(self):
        self.c.execute("SELECT slug FROM platforms WHERE genre_crawl_complete=0")
        return self.c.fetchall()

    def get_remaining_title_crawls(self):
        self.c.execute("SELECT url, last_page_scraped, final_page_number FROM platform_genre_crawls")
        crawls = self.c.fetchall()
        return [_c for _c in crawls if _c[1] != _c[2]]

    def get_one_incomplete_game_crawl(self):
        self.c.execute("SELECT * FROM games "
                       "WHERE final_user_review_page_number=NULL"
                       "OR final_critic_review_page_number=NULL "
                       "LIMIT=20")
        result = self.c.fetchall()
        if not result:
            self.c.execute("SELECT * FROM games "
                           "WHERE last_user_review_page_scraped!=final_user_review_page_number"
                           "OR last_critic_review_page_scraped!=final_critic_review_page_number "
                           "LIMIT=20")
            result = self.c.fetchall()
            if not result:
                return False
        result = choice(result)
        if result[5] > result[6]:
            page = 0 if not result[6] else result[6] + 1
            crawl = []
            crawl.append("user")
        elif result[7] > result[8]:
            page = 0 if not result[8] else result[8] + 1
            crawl = []
            crawl.append("critic")
        





    # Existence checks - return rowid if exists else False
    def platform_exists(self, slug: str):
        self.execute("SELECT rowid FROM platforms WHERE slug=?", (slug,))
        result = self.c.fetchone()
        return result[0] if result else False

    def game_exists(self, slug, platform):
        platform_pk = self.platform_exists(platform)
        self.c.execute("SELECT rowid FROM games WHERE slug=? AND platform=?",
                       (slug, platform_pk))
        result = self.c.fetchone()
        return result[0] if result else False

    def user_review_exists(self, review_id: str):
        self.c.execute("SELECT rowid FROM user_reviews WHERE review_id=?",
                       (review_id,))
        result = self.c.fetchone()
        return result[0] if result else False

    def critic_review_exists(self, author, date):
        self.c.execute("SELECT rowid FROM critic_reviews WHERE author=? AND date=?",
                       (author, date))
        result = self.c.fetchone()
        return result[0] if result else False

    def platform_genre_crawl_url_exists(self, url):
        self.c.execute("SELECT rowid FROM platform_genre_crawls WHERE url=?",
                       (url,))    
        result = self.c.fetchone()
        return result[0] if result else False
    
    # Row insertion
    def new_game(self, title, slug, platform_slug, released) -> bool:
        if self.game_exists(slug, platform_slug):
            return False
        platform_pk = self.platform_exists(platform_slug)
        self.c.execute("INSERT INTO games (title, slug, platform, released) \
                       VALUES (?,?,?,?)",
                       (title, slug, platform_pk, released))
        self.conn.commit()
        return self.c.lastrowid

    def new_critic_review(self, game_pk, review_id, author, date, grade, body):
        if self.critic_review_exists(author, date):
            return False
        game_pk = self.game_exists(game_slug)
        self.c.execute("INSERT INTO critic_reviews "
                       "(game, review_id, author, date, grade, body) "
                       "VALUES (?,?,?,?,?,?)",
                       (game_pk, review_id, author, date, grade, body))
        self.conn.commit()

    def new_user_review(self, game_slug, review_id, author, date, grade, body,
                              votes_total, votes_helpful):
        if self.user_review_exists(review_id):
            return False
        game_pk = self.game_exists(game_slug)
        self.c.execute("INSERT INTO critic_reviews "
                       "(game, review_id, author, date, grade, body) "
                       "VALUES (?,?,?,?,?,?)",
                       (game_pk, review_id, author, date, grade, body))
        self.conn.commit()

    def new_platform_genre_crawl_url(self, platform_pk, url: str):
        if self.platform_genre_crawl_url_exists(url):
            return False
        self.c.execute("INSERT INTO platform_genre_crawls (platform, url) "\
                       "VALUES (?,?)", (platform_pk, url,))
        self.conn.commit()

    # Progress tracking methods - final_page_number and last_page_scraped
    def add_final_review_page_number(self, game_pk, review_type: str, number: int):
        if review_type not in ["critic", "user"]:
            raise RuntimeError
        self.c.execute(f"UPDATE games "
                        "SET final_{review_type}_review_page_number=? "
                        "WHERE rowid=?", (number))
        self.conn.commit()

    def update_last_review_page_scraped(self, game_pk, review_type: str, number: int):
        if review_type not in ["critic", "user"]:
            raise RuntimeError
        self.c.execute(f"UPDATE games "
                        "SET last_{review_type}_review_page_scraped=? "
                        "WHERE rowid=?", (number))
        self.conn.commit()

    def add_final_platform_genre_page_number(self, url, number):
        self.c.execute("UPDATE platform_genre_crawls "
                       "SET final_page_number=? WHERE url=?",
                       (number, url))
        self.conn.commit()

    def update_last_platform_genre_page_scraped(self, url, number):
        self.c.execute("UPDATE platform_genre_crawls "
                       "SET last_page_scraped=? WHERE url=?",
                       (number, url))
        self.conn.commit()
