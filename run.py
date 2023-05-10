import tweepy
import configparser
import time
import json
from sqlite3 import Error
import sqlite3


config = configparser.ConfigParser()
config.read('conf.ini')
twitter_consumer_key = config['TWITTER']['twitter_consumer_key']
twitter_consumer_secret = config['TWITTER']['twitter_consumer_secret']
twitter_access_token = config['TWITTER']['twitter_access_token']
twitter_access_token_secret = config['TWITTER']['twitter_access_token_secret']
screen_names = [i.strip() for i in config['SETTINGS']['screen_names'].split(',')]
get_last_tweet_date = config['SETTINGS'].getboolean('test_mode')

auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)


def db_connect():
    try:
        conn = sqlite3.connect('users.db')
        create_table = """CREATE TABLE IF NOT EXISTS users (
                                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        account TEXT NOT NULL,
                                        type TEXT NOT NULL,
                                        user_id TEXT NOT NULL,
                                        username TEXT NOT NULL,
                                        screen_name TEXT NOT NULL,
                                        last_tweet_date TEXT NULL
                                        );"""
        conn.execute(create_table)
        return conn
    except Error as e:
        print(e)
    return None


def insert_row(conn, account, type, user_id, username, screen_name, last_tweet_date):
    conn.execute("INSERT INTO users (account, type, user_id, username, screen_name, last_tweet_date) VALUES (?, ?, ?, ?, ?, ?);", (account, type, user_id, username, screen_name, last_tweet_date))


def get_last_tweet_date(screen_name):
    last_tweet_date = False
    try:
        tweets = api.user_timeline(screen_name=screen_name, count=1, exclude_replies=True, include_rts=False)

        for tweet in tweets:
            last_tweet_date = tweet.created_at
    except Exception as e:
        print(e)
    return last_tweet_date


def get_twitter_peeps(conn, screen_name, mode, get_last_tweet_date):
    all_data = []

    if mode == 'following':
        peeps = api.get_friends
    if mode == 'followers':
        peeps = api.get_followers

    print(f'Gathering {mode} of {screen_name}')

    for page in tweepy.Cursor(peeps, screen_name=screen_name, count=200).pages():
        for user in page:
            last_tweet_date = None
            if get_last_tweet_date:
                last_tweet_date = get_last_tweet_date(user.screen_name)
            print(user.name, last_tweet_date)
            insert_row(conn, screen_name, mode, user.id, user.name, user.screen_name, last_tweet_date)
        conn.commit()


def main():
    conn = db_connect()
    for screen_name in screen_names:
        followers = get_twitter_peeps(conn, screen_name, 'followers', get_last_tweet_date)

        following = get_twitter_peeps(screen_name, 'friends', get_last_tweet_date)
        write_json('following', following)


if __name__ == '__main__':
    main()
