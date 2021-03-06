import re
from typing import List

from bs4 import BeautifulSoup


BASE_URL = "https://www.metacritic.com"

# Methods for listing pages (pages using "flipper" divs for next/prev page)
def get_title_by_genre_listing_page_urls(soup: BeautifulSoup) -> List[str]:
    listings = soup.find("ul", {"class": "genre_nav"}).find_all("a")
    return [f"{BASE_URL}{listing.attrs['href']}" for listing in listings]


def get_last_page_number(listing_page: BeautifulSoup) -> str(int):
    last_page_tag = listing_page.find("li", {"class": "last_page"})
    if not last_page_tag:
        return 0
    return last_page_tag.find("a").attrs["href"].split("page=")[-1]


# Scraping methods
def scrape_games(title_by_genre_listing_page: BeautifulSoup) -> List:
    games = []
    soup = title_by_genre_listing_page
    for game_tag in soup.find_all("td", {"class": "clamp-summary-wrap"}):
        games.append({
            "title": game_tag.find("h3").text,
            "slug": game_tag.find("a", {"class": "title"})\
                                    .attrs["href"].split("/")[-1],
            "released": game_tag.find("div", {"class": "clamp-details"})\
                                .find_all("span")[-1].text,
            "metascore": int(game_tag.find("div", {"class": "metascore_w"}).text)
            })
    return games


def scrape_user_reviews(user_review_listing_page: BeautifulSoup) -> List:
    reviews = []
    soup = user_review_listing_page
    for rev_tag in soup.find_all("li", {"class": "user_review"}):
        review = {
            "review_id": int(rev_tag["id"].split("_")[-1]),
            "author": rev_tag.find("div", {"class": "name"}).text.strip(),
            "date": rev_tag.find("div", {"class": "date"}).text,
            "grade": int(rev_tag.find("div", {"class": "metascore_w"}).text),
            "votes_total": int(rev_tag.find("span", {"class": "total_thumbs"}).text),
            "votes_helpful": int(rev_tag.find("span", {"class": "total_ups"}).text)
        }
        body_tag = rev_tag.find("div", {"class": "review_body"})
        if body_tag.find("span", {"class": "blurb_expanded"}):     # longer reviews
            body = body_tag.find("span", {"class": "blurb_expanded"})
        else:                                   # short reviews
            body = body_tag.find("span")
        body = "" if body == None else body.text.strip().replace("\r", " ").replace("\n", " ")
        review["body"] = re.sub(" +", " ", body)
        reviews.append(review)
    return reviews


def scrape_critic_reviews(critic_review_listing_page: BeautifulSoup) -> List:
    reviews = []
    soup = critic_review_listing_page
    for rev_tag in soup.find_all("li", {"class": "critic_review"}):
        review = {
            "author": rev_tag.find("div", {"class": "source"}).text,
            "body": rev_tag.find("div", {"class": "review_body"}).text.strip()
        }
        grade = rev_tag.find("div", {"class": "metascore_w"})
        review["grade"] = None if not grade or not grade.text else grade.text
        date = rev_tag.find("div", {"class": "date"})
        review["date"] = None if not date or not date.text else date.text
        reviews.append(review)
    return reviews
