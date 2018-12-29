import os
import sqlite3
import random
import hashlib
from time import mktime
from datetime import datetime
import feedparser
import yaml
import logging
import telepot
from bs4 import BeautifulSoup as bs

logging.basicConfig(level=logging.INFO)


class NewsCollector(object):

    def __init__(self):
        with open("config.yaml", "r") as infile:
            config = yaml.load(infile.read())["rss"]
        self.settings = config["settings"]
        self.urls = config["urls"]
        self.bot = telepot.Bot(config["settings"]["token"])

        # Subscribers
        self.subscribers = config["subscribers"]

        # Database connection
        self.db = sqlite3.connect(config["database"]["name"])
        self.db_table = config["database"]["table"]
        self.cursor = self.db.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS {} (
                hash TEXT PRIMARY KEY,
                feed TEXT,
                feed_url TEXT,
                title TEXT,
                description TEXT,
                link TEXT,
                date TEXT,
                SENT INTEGER DEFAULT 0
            )
        """.format(self.db_table))
        self.db.commit()

    def _get_publish_datetime(self, entry):
        timestamp = entry.get("updated_parsed", None)
        if timestamp is None:
            timestamp = entry.get("published_parsed", None)
        if timestamp is None:
            return ""
        timestamp = datetime.fromtimestamp(mktime(timestamp))
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def collect_news(self):
        # Collect news from RSS feeds
        news = []

        for url in self.urls:
            logging.info("Collecting from {}...".format(url))
            feed = feedparser.parse(url)
            feed_title = feed["feed"]["title"]
            feed_url = feed["feed"]["link"]
            for entry in feed["entries"]:
                md5 = hashlib.md5()
                md5.update(entry["title"].lower().encode("utf-8"))
                date = self._get_publish_datetime(entry)
                news.append((
                    md5.hexdigest(),
                    feed_title,
                    feed_url,
                    entry["title"],
                    entry.get("description", ""),
                    entry["link"],
                    date,
                    0
                ))

        logging.info("Collected {} entries from {} RSS feeds".format(
            len(news), len(self.urls)))

        query = """
            INSERT OR IGNORE INTO {}
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """.format(self.db_table)
        self.cursor.executemany(query, news)
        self.db.commit()
        logging.info("New entries inserted into database")

    def send_news(self):
        query = """
            SELECT * FROM {}
            WHERE SENT = 0
            ORDER BY date DESC
            LIMIT 200
        """.format(self.db_table, self.settings["daily_news"])
        news = list(self.cursor.execute(query))
        random.shuffle(news)
        news = news[:self.settings["daily_news"]]

        messages = []
        messages.append("ML News of the Day {}".format(
            datetime.now().strftime("%Y-%m-%d")))

        for n in news:
            title = bs(n[3], "lxml").text.strip()
            description = " ".join(
                bs(n[4], "lxml").text.split(" ")[:40]).strip()
            message = "[{}]({})\n{} - {}\n> {} ...\n\n".format(
                title, n[5], n[6][:10], n[1], description)
            messages.append(message)

        # Set message as send
        query = """
            UPDATE {}
            SET SENT = 1
            WHERE hash = ?
        """.format(self.db_table)
        for n in news:
            self.cursor.execute(query, (n[0],))
        self.db.commit()

        # Send message to telegram subscribers
        for chat_id in self.subscribers:
            logging.info("Sending messages to {}".format(chat_id))
            for m in messages:
                self.bot.sendMessage(chat_id, m, parse_mode='Markdown')


if __name__ == "__main__":

    collector = NewsCollector()
    collector.collect_news()
    collector.send_news()
