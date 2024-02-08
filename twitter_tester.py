from twitter_sqlite import TwitterAPI
import os

def __main__():
    path = "hw1_data/twitter_db"

    if os.path.exists(path):
        os.remove(path)
    api = TwitterAPI(path)

    print(api.postTweets())
    print(api.getHomeTimeline())
    print(api.getFollowees(api.my_user_id()))
    print(api.getFollowers(api.my_user_id()))
    print(api.getTweets(api.my_user_id()))

if __name__ == '__main__':
    __main__()

