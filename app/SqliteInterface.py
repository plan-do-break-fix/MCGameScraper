import os
from random import choice
import sqlite3
from typing import List


PLATFORMS = [
    "pc",
    "playstation-2",
    "playstation-3",
    "playstation-4",
    "playstation-5",
    "switch",
    "xbox-360",
    "xbox-one",
    "wii-u"
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
SCHEMAS = [
    "CREATE TABLE IF NOT EXISTS 'platforms' ("
    "  slug TEXT NOT NULL,"
    "  genre_crawl_complete INTEGER DEFAULT 0"
    ");"
    ,
    "CREATE TABLE IF NOT EXISTS 'genres' ("
    "  slug TEXT NOT NULL"
    ");"
    ,
    "CREATE TABLE IF NOT EXISTS 'games' ("
    "  title TEXT NOT NULL,"
    "  slug TEXT NOT NULL,"
    "  platform INTEGER NOT NULL,"
    "  released TEXT NOT NULL,"
    "  metascore INTEGER DEFAULT NULL,"
    "  final_user_review_page_number INTEGER DEFAULT NULL,"
    "  last_user_review_page_scraped INTEGER DEFAULT NULL,"
    "  final_critic_review_page_number INTEGER DEFAULT NULL,"
    "  last_critic_review_page_scraped INTEGER DEFAULT NULL,"
    "  FOREIGN KEY (platform) REFERENCES platforms (rowid)"
    ");"
    ,
    "CREATE TABLE IF NOT EXISTS 'games_to_genres' ("
    "  game INTEGER NOT NULL,"
    "  genre INTEGER NOT NULL"
    ");"
    ,
    "CREATE TABLE IF NOT EXISTS 'critic_reviews' ("
    "  game INTEGER NOT NULL,"
    "  author TEXT NOT NULL,"
    "  date TEXT DEFAULT NULL,"
    "  grade INTEGER DEFAULT NULL,"
    "  body TEXT NOT NULL,"
    "  FOREIGN KEY (game) REFERENCES games (rowid)"
    ");"
    ,
    "CREATE TABLE IF NOT EXISTS 'user_reviews' ("
    "  game INTEGER NOT NULL,"
    "  review_id INTEGER NOT NULL,"
    "  author TEXT NOT NULL,"
    "  date TEXT NOT NULL,"
    "  grade INTEGER DEFAULT NULL,"
    "  body TEXT NOT NULL,"
    "  votes_total INTEGER NOT NULL,"
    "  votes_helpful INTEGER NOT NULL,"
    "  FOREIGN KEY (game) REFERENCES games (rowid)"
    ");"
    ,
    "CREATE TABLE IF NOT EXISTS 'platform_genre_crawls' ("
    "  platform INTEGER NOT NULL,"
    "  genre INTEGER NOT NULL,"
    "  url STRING DEFAULT NULL,"
    "  final_page_number INTEGER DEFAULT NULL,"
    "  last_page_scraped INTEGER DEFAULT NULL,"
    "  FOREIGN KEY (platform) REFERENCES platforms (rowid)"
    ");"
] 



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
        for platform in PLATFORMS:
            self.c.execute("INSERT INTO platforms (slug) VALUES (?)", (platform,)) 
        for genre in GENRES:
            self.c.execute("INSERT INTO genres (slug) VALUES (?)", (genre,))
        self.conn.commit()

    #def get_title_by_genre_crawls_without_final_page(self) -> List:
    #    self.c.execute("SELECT url FROM platform_genre_crawls WHERE final_page_number IS NULL")
    #    return [_i[0] for _i in self.c.fetchall()]

    def get_platform_slug(self, platform_pk) -> str:
        self.c.execute("SELECT slug FROM platforms WHERE rowid=?", (platform_pk,))
        return self.c.fetchone()[0]

    def get_game_slug(self, game_pk) -> str:
        self.c.execute("SELECT slug FROM games WHERE rowid=?", (game_pk,))
        return self.c.fetchone()[0]

    def get_platform_pk_from_genre_crawl_url(self, url):
        self.c.execute("SELECT platform FROM platform_genre_crawls WHERE url=?",
                       (url,))
        return self.c.fetchone()[0]
    
    def get_genre_pk_from_genre_crawl_url(self, url):
        self.c.execute("SELECT genre FROM platform_genre_crawls WHERE url=?",
                       (url,))
        return self.c.fetchone()[0]

    def get_platforms_without_genre_crawl_urls(self):
        self.c.execute("SELECT slug FROM platforms WHERE genre_crawl_complete=0")
        return [_i[0] for _i in self.c.fetchall()]

    def get_one_incomplete_last_page_crawl(self):
        self.c.execute("SELECT url FROM platform_genre_crawls "
                       "WHERE final_page_number IS NULL "
                       "LIMIT 10")
        result = self.c.fetchall()
        if not result:
            return False
        return choice(result)[0]

    def get_one_incomplete_title_crawl(self):
        self.c.execute("SELECT * FROM platform_genre_crawls "
                       "WHERE last_page_scraped IS NULL "
                       "OR last_page_scraped < final_page_number "
                       "LIMIT 10")
        result = self.c.fetchall()
        if not result:
            return False
        result = choice(result)
        page = 0 if result[4] == None else result[4] + 1
        return [result[2], page]

    def get_one_incomplete_game_crawl(self):
        self.c.execute("SELECT * FROM games "
                       "WHERE final_user_review_page_number IS NULL "
                       "OR final_critic_review_page_number IS NULL "
                       "OR last_user_review_page_scraped < final_user_review_page_number "
                       "OR last_critic_review_page_scraped < final_critic_review_page_number "
                       "LIMIT 20")
        result = self.c.fetchall()
        if not result:
            return False
        result = choice(result)
        if not result[6] or result[6] < result[5]:
            page = 0 if result[6] == None else result[6] + 1
            review_type = "user"
        elif not result[8] or result[8] < result[7]:
            page = 0 if result[8] == None else result[8] + 1
            review_type = "critic"
        platform_slug = self.get_platform_slug(result[2])
        game_pk = self.game_exists(result[1], result[2])
        crawl = [game_pk, result[1], result[2], platform_slug, page, review_type]
        return crawl

    # Existence checks - return rowid if exists else False
    def platform_exists(self, slug: str):
        self.c.execute("SELECT rowid FROM platforms WHERE slug=?", (slug,))
        result = self.c.fetchone()
        return result[0] if result else False

    def genre_exists(self, slug: str):
        self.c.execute("SELECT rowid FROM genres WHERE slug=?", (slug,))
        result = self.c.fetchone()
        return result[0] if result else False

    def game_exists(self, slug, platform_pk):
        self.c.execute("SELECT rowid FROM games WHERE slug=? AND platform=?",
                       (slug, platform_pk))
        result = self.c.fetchone()
        return result[0] if result else False

    def game_genre_association_exists(self, game_pk, genre_pk):
        self.c.execute("SELECT rowid FROM games_to_genres WHERE game=? AND genre=?",
                       (game_pk, genre_pk))

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
    def new_game(self, title, slug, platform, released, metascore) -> bool:
        if self.game_exists(slug, platform):
            return False
        self.c.execute("INSERT INTO games (title, slug, platform, released, metascore) "
                       "VALUES (?,?,?,?,?)",
                       (title, slug, platform, released, metascore))
        self.conn.commit()
        return self.c.lastrowid

    def new_game_genre_association(self, game_pk, genre_pk):
        if self.game_genre_association_exists(game_pk, genre_pk):
            return False
        self.c.execute("INSERT INTO games_to_genres (game, genre) VALUES (?,?)",
                       (game_pk, genre_pk))

    def new_critic_review(self, game_pk, author, date, grade, body):
        if self.critic_review_exists(author, date):
            return False
        self.c.execute("INSERT INTO critic_reviews "
                       "(game, author, date, grade, body) "
                       "VALUES (?,?,?,?,?)",
                       (game_pk, author, date, grade, body))
        self.conn.commit()

    def new_user_review(self, game_pk, review_id, author, date, grade, body,
                              votes_total, votes_helpful):
        if self.user_review_exists(review_id):
            return False
        self.c.execute("INSERT INTO user_reviews "
                       "(game, review_id, author, date, grade, body, votes_total, votes_helpful) "
                       "VALUES (?,?,?,?,?,?,?,?)",
                       (game_pk, review_id, author, date, grade, body, votes_total, votes_helpful))
        self.conn.commit()

    def new_platform_genre_crawl_url(self, platform_pk, genre_pk, url: str):
        if self.platform_genre_crawl_url_exists(url):
            return False
        self.c.execute("INSERT INTO platform_genre_crawls (platform, genre, url) "
                       "VALUES (?,?,?)", (platform_pk, genre_pk, url))
        self.conn.commit()

    # Progress tracking methods - final_page_number and last_page_scraped
    def update_genre_crawl_complete(self, platform_slug):
        self.c.execute("UPDATE platforms SET genre_crawl_complete=1 "
                       "WHERE slug=?", (platform_slug,))
        self.conn.commit()
    
    def add_final_review_page_number(self, game_pk, review_type: str, number: int):
        if review_type not in ["critic", "user"]:
            raise RuntimeError
        self.c.execute("UPDATE games "
                       f"SET final_{review_type}_review_page_number=? "
                       "WHERE rowid=?", (number, game_pk))
        self.conn.commit()

    def update_last_review_page_scraped(self, game_pk, review_type: str, number: int):
        if review_type not in ["critic", "user"]:
            raise RuntimeError
        self.c.execute("UPDATE games "
                       f"SET last_{review_type}_review_page_scraped=? "
                       "WHERE rowid=?", (number, game_pk))
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
