#!bin/python3

import sqlite3
from langdetect import detect, LangDetectException

from app import SqliteInterface

def make_language_table():
    db = SqliteInterface.Interface(".")
    db.c.execute("CREATE TABLE IF NOT EXISTS language ("
                 "  lang TEXT NOT NULL,"
                 "  review INTEGER NOT NULL"
                 ");")
    db.c.execute("SELECT * FROM user_reviews")
    c_write = db.conn.cursor()
    failed = []
    for _r in db.c:
        review_id = _r[1]
        body = _r[5]
        try:
            lang = detect(body)
        except LangDetectException:
            failed.append(str(review_id))
            print("FAILURE")
            continue
        c_write.execute("INSERT INTO language (lang, review) VALUES (?, ?)",
                                            (lang, review_id))
        db.conn.commit()
        print(f"Review {review_id} - {lang} detected")
    print(failed)

def makedb_simple_binary():
    db_in = SqliteInterface.Interface(".")
    db_out = sqlite3.connect("lps.userReviews.simpleBinary.sqlite3.db")
    c_out = db_out.cursor()
    schema = "CREATE TABLE IF NOT EXISTS {} ("\
             "  review_id INTEGER NOT NULL,"\
             "  review TEXT NOT NULL"\
             ");"
    for _i in ["positive", "negative"]:
        _schema = schema.format(_i)
        c_out.execute(_schema)
    db_out.commit()
    db_in.c.execute("SELECT grade, review_id, body FROM user_reviews"\
                 "  WHERE 'en' IN"\
                 "  (SELECT lang FROM language WHERE review=review_id);")
    for _r in db_in.c:
        if 4 <= _r[0] <= 6:
            continue
        elif _r[0] < 4:
            _tbl = "negative"
        elif _r[0] > 6:
            _tbl = "positive"
        c_out.execute(f"INSERT INTO {_tbl}"
                       "  (review_id, review)"
                       "  VALUES (?, ?)", 
                       (_r[1], _r[2]))

        db_out.commit()
        print(f"{_tbl.title()} review added to output.")