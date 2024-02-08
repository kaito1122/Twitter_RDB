"""
Twitter Database API for MySQL
"""

from dbutils import DBUtils
import csv
import random as rand
from datetime import datetime
from perf_tester import perf_tester


class TwitterAPI:

    def __init__(self, path):
        # go to the path database. if not exist, create one
        self.dbu = DBUtils(path)

        # create tweet table and follows table
        self.dbu.create_table("CREATE TABLE tweet( \
	                            tweet_id INTEGER PRIMARY KEY AUTOINCREMENT, \
	                            user_id INTEGER NOT NULL, \
	                            tweet_ts DATETIME, \
	                            TWEET_TEXT varchar(140) \
                                );")
        self.dbu.create_table("CREATE TABLE follows( \
                                user_id INTEGER NOT NULL, \
                                follows_id INTEGER NOT NULL \
                                );")

        # add follows table content
        self.account_added()

    def my_user_id(self):
        """ randomly selected twitter user id for a test login use """
        sql_script = "SELECT user_id FROM tweet"
        user_ids = list(self.dbu.execute(sql_script)['user_id'])
        return rand.choice(user_ids)

    @perf_tester
    def postTweets(self):
        """ posts tweet one at a time to tweet table """
        sql_script = "INSERT INTO tweet (user_id, tweet_ts, tweet_text) VALUES (?, ?, ?)"
        tweet_data = "hw1_data/tweet.csv"

        with open(tweet_data, 'r') as f:
            # read csv file, separated by commas
            reader = csv.reader(f, delimiter=',')
            # skips the header
            next(reader)
            for tweets in reader:
                # insert the user_id, tweet_ts, and tweet_text. Note that tweet_id is auto incremented
                load_info = (tweets[0], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), tweets[1])
                self.dbu.insert_one(sql_script, load_info)

    # to run it only one time and see the timeline, comment off @perf_tester
    @perf_tester
    def getHomeTimeline(self, login=1):
        """ gets 10 random tweets by the user's followees in the most recent order """
        # get 10 random followees for timeline. Ignore the "Followees:" please
        user_list = self.getFollowees(login)
        chosen = tuple(rand.choices(list(user_list['follows_id']), k=10))

        sql_script = "SELECT  user_id, tweet_text \
                        FROM tweet \
                        WHERE user_id IN {} \
                        ORDER BY tweet_ts DESC \
                        LIMIT 10; ".format(chosen)

        return self.dbu.execute(sql_script)

    def account_added(self):
        """ loads the follows.csv content to follows table programmatically """
        sql_script = "INSERT INTO follows VALUES (?, ?)"
        follows_data = "hw1_data/follows.csv"

        with open(follows_data, 'r') as f:
            # reads csv file, separated by commas
            reader = csv.reader(f, delimiter=',')
            # skips header
            next(reader)
            lst = []
            for accounts in reader:
                lst.append(accounts)
            self.dbu.insert_many(sql_script, lst)

    def getFollowees(self, login=1):
        """ gets the list of followees by logged in random user """
        print('Logged in as user_id: ', login)
        print('Followees:')

        sql_script = "SELECT follows_id \
                        FROM follows \
                        WHERE user_id = {}; ".format(login)
        return self.dbu.execute(sql_script)

    def getFollowers(self, login=1):
        """ gets the list of followers by logged in random user """
        print('Logged in as user_id: ', login)
        print('Followers:')

        sql_script = "SELECT user_id  \
                        FROM follows \
                        WHERE follows_id = {}; ".format(login)
        return self.dbu.execute(sql_script)

    def getTweets(self, login=1):
        """ gets the list of tweets by logged in random user """
        print('Logged in as user_id: ', login)
        print('Tweets:')

        sql_script = "SELECT tweet_text \
                      FROM tweet \
                      WHERE user_id = {}".format(login)
        return self.dbu.execute(sql_script)
