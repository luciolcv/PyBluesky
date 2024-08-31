# Libraries
from atproto import Client, models
import sys
import time
import json
from datetime import datetime, timezone

# Some constants
TIME_SLEEP_POLITENESS = 0.2  # seconds


class RateLimitedClient(Client):
    """
    Originally got it from https://github.com/MarshalX/atproto/discussions/167
    """
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._limit = self._remaining = self._reset = None

    def get_rate_limit(self):
        return {'limit':self._limit,
                'remaining':self._remaining, 
                'reset':self._reset}

    def _invoke(self, *args, **kwargs):
        response = super()._invoke(*args, **kwargs)

        self._limit = response.headers.get('RateLimit-Limit')
        self._remaining = response.headers.get('RateLimit-Remaining')
        self._reset = response.headers.get('RateLimit-Reset')

        return response

 
def login(username, password):
    """
    Login to the Bsky.social server.
    :param username: Username
    :param password: Password
    :return: Client object
    """
    # Creating the client
    at_client = RateLimitedClient()

    # Preparing handle (bsky.social is the default, yet it is better to specify it, in case of other servers)
    handle = "{}.bsky.social".format(username)

    # Logging in
    try:
        at_client.login(handle, password)
        print("[+] Logged in as {}".format(handle))
        print("[+] Current rate limits: {}".format(at_client.get_rate_limit()))
        return at_client
    except Exception as e:
        # Printing the error
        print(f"[-] Something went wrong while logging in as {handle}")
        print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
        return None


def get_time_to_next_reset(t):
    """
    Gets the waiting time (in seconds) until the next rate limit reset
    """
    # Time provided by Bluesky is UNIX time
    refresh_time = datetime.datetime.fromtimestamp(t, timezone.utc)
    current_time = datetime.now(timezone.utc)

    if refresh_time > current_time:
        waiting_time = refresh_time - current_time
        return waiting_time.seconds
    else:
        return 0


def check_rate_limits(at_client, tolerance=10):
    """
    Checking if the rate limit is approaching to avoid reaching it
    at_client: the client object
    tolerance: tolerance w.r.t. the number of remaining queries (which corresponds to the number of threads or a fixed number)
    """

    # Getting all the rate limit info
    rate_limit_info = at_client.get_rate_limit()

    # Getting the number of remaining queries
    remaining_queries = rate_limit_info['remaining']

    # Getting the time to reset
    rate_limit_reset = rate_limit_info['reset']

    # Checking if the number of remaining queries is less than the tolerance
    if int(remaining_queries) <= tolerance:
        waiting_time = get_time_to_next_reset(rate_limit_reset)
        if waiting_time > 0:
            print(f"[{datetime.now()}] Rate limit reached, waiting for {waiting_time} sec.")
            time.sleep(waiting_time)
            print(f"[{datetime.now()}] Rate limit ok!")


def get_user_posts(at_client, user_screen_name, max_posts=1000000, limit=100):
    """
    Get user's posts.
    :param at_client: Client object
    :param user_screen_name: User's screen name
    :return: List of posts
    """
    # Preparing user handle
    user_handle = "{}.bsky.social".format(user_screen_name)

    # Preparing cursor
    cursor = None

    # Initializing the list of posts
    feeds = []

    # Initially condition is true (i.e., current_size < max_posts or cursor will not be None)
    condition = True

    # Debug variables
    num_calls = 0

    # Get profile's posts using the cursor as a paginator, max_posts+eps (i.e., the whole call) posts will be retrieved
    while condition:
        try:
            # Before making the call, we check the rate limit
            check_rate_limits(at_client)

            # Preparing the parameters
            params = models.AppBskyFeedGetAuthorFeed.Params(actor=user_handle,
                                                            cursor=cursor,
                                                            limit=limit)

            # Getting the author feed
            profile_feed = at_client.app.bsky.feed.get_author_feed(params)

            # Appending the posts to the list
            if profile_feed.feed is not None and len(profile_feed.feed) > 0:
                for feed_view in profile_feed.feed:
                    feeds.append(json.loads(feed_view.model_dump_json()))
            # Debug only:
            # print(f"[+] Got {len(profile_feed.feed)} new posts for {user_screen_name} | Total = {len(feeds)}")

            # Debug
            # Printing call number every 500 calls
            if num_calls % 100 == 0 and num_calls > 0:
                print(f"[*] Call number: {num_calls} for {user_screen_name}")


            # Updating the cursor
            cursor = profile_feed.cursor

            # Updating the condition
            condition = (cursor is not None) and (len(feeds) < max_posts)

            if not condition:
                # If we have crawled no posts, we return None
                if len(feeds) == 0:
                    return None

                # print(f"[+] Got {len(feeds)} posts for {user_screen_name}")

                # Otherwise, we return them
                return {user_handle: feeds}
            
            # Being polite
            time.sleep(TIME_SLEEP_POLITENESS)

        except Exception as e:
            # Printing the error
            print(f"[-] Something went wrong while getting the posts for {user_screen_name}")
            print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
            return None


def get_user_followers(at_client, user_screen_name, max_followers=100000000, limit=100):
    """
    Get user's followers.
    :param at_client: Client object
    :param user_screen_name: User's screen name
    :return: List of followers
    """
    # Preparing user handle
    user_handle = "{}.bsky.social".format(user_screen_name)

    # Preparing cursor
    cursor = None

    # Preparing list of followers
    followers = []

    # Initially condition is true (i.e., current_size < max_posts or cursor will not be None)
    condition = True

    # Rate limit variables
    num_calls = 0

    # Get user's followers using the cursor as a paginator, max_followers+eps (i.e., the whole call) followers will be retrieved
    while condition:
        try:
            # Before making the call, we check the rate limit
            check_rate_limits(at_client)

            # Preparing the parameters
            params = models.AppBskyGraphGetFollowers.Params(actor=user_handle,
                                                            cursor=cursor,
                                                            limit=limit)

            # Getting the followers
            response_followers = at_client.app.bsky.graph.get_followers(params)

            # Appending the followers to the list
            if response_followers.followers is not None and len(response_followers.followers) > 0:
                followers.extend(response_followers.followers)
            # Debug only
            # print(f"[+] Got {len(response_followers.followers)} new followers for {user_screen_name} | Total = {len(followers)}")

            # Debug
            # Printing call number every 500 calls
            if num_calls % 100 == 0 and num_calls > 0:
                print(f"[*] Call number: {num_calls} for {user_screen_name}")

            # Updating the cursor
            cursor = response_followers.cursor

            # Updating the condition
            condition = (cursor is not None) and (len(followers) < max_followers)

            if not condition:
                # If we have crawled no followers
                if len(followers) == 0:
                    return

                print(f"[+] Got {len(followers)} followers for {user_screen_name}")

                # Otherwise, we parse the followers and return them
                parsed_followers = [dict(follower) for follower in followers]
                return {user_handle: parsed_followers}
            
            # Being polite
            time.sleep(TIME_SLEEP_POLITENESS)

            # Checking the rate limit
            num_calls += 1

        except Exception as e:
            # Printing the error
            print(f"[-] Something went wrong while getting the followers of {user_screen_name}")
            print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
            return None


def get_user_follows(at_client, user_screen_name, max_follows=100000000, limit=100):
    """
    Get user's follows.
    :param at_client: Client object
    :param user_screen_name: User's screen name
    :return: List of followers
    """
    # Preparing user handle
    user_handle = "{}.bsky.social".format(user_screen_name)

    # Preparing cursor
    cursor = None

    # Preparing list of followers
    follows = []

    # Initially condition is true (i.e., current_size < max_posts or cursor will not be None)
    condition = True

    # Rate limit variables
    num_calls = 0

    # Get user's follows using the cursor as a paginator, max_followers+eps (i.e., the whole call) followers will be retrieved
    while condition:
        try:
            # Before making the call, we check the rate limit
            check_rate_limits(at_client)

            # Preparing the parameters
            params = models.AppBskyGraphGetFollows.Params(actor=user_handle,
                                                          cursor=cursor,
                                                          limit=limit)

            # Getting the follows
            response_follows = at_client.app.bsky.graph.get_follows(params)

            # Appending the followers to the list
            if response_follows.follows is not None and len(response_follows.follows) > 0:
                follows.extend(response_follows.follows)
            # Debug only
            # print(f"[+] Got {len(response_follows.follows)} new follows for {user_screen_name} | Total = {len(follows)}")

            # Debug
            # Printing call number every 500 calls
            if num_calls % 100 == 0 and num_calls > 0:
                print(f"[*] Call number: {num_calls} for {user_screen_name}")

            # Updating the cursor
            cursor = response_follows.cursor

            # Updating the condition
            condition = (cursor is not None) and (len(follows) < max_follows)

            if not condition:
                # If we have crawled no follows
                if len(follows) == 0:
                    return

                print(f"[+] Got {len(follows)} follows for {user_screen_name}")

                # Otherwise, we parse the follows and return them
                parsed_follows = [dict(follow) for follow in follows]
                return {user_handle: parsed_follows}
            
            # Being polite
            time.sleep(TIME_SLEEP_POLITENESS)
            
            # Checking the rate limit
            num_calls += 1

        except Exception as e:
            # Printing the error
            print(f"[-] Something went wrong while getting the follows of {user_screen_name}")
            print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
            return None


def get_posts_from_query(at_client, query, max_posts=100000000, limit=100):
    """
    Get posts containing a given query
    :param at_client: Client object
    :param query: The query to search for
    :return: List of posts
    """
    # Preparing cursor
    cursor = None

    # Initializing the list of posts
    posts = []

    # Initially condition is true (i.e., current_size < max_posts or cursor will not be None)
    condition = True

    # Debug variables
    num_calls = 0

    # Get profile's posts using the cursor as a paginator, max_posts+eps (i.e., the whole call) posts will be retrieved
    while condition:
        try:
            # Before making the call, we check the rate limit
            check_rate_limits(at_client)

            # Preparing the parameters
            params = models.AppBskyFeedSearchPosts.Params(cursor=cursor, 
                                                          q=query, 
                                                          limit=limit)


            # Getting the author feed
            retrieved_posts = at_client.app.bsky.feed.search_posts(params)


            # Appending the posts to the list
            if retrieved_posts.posts is not None and len(retrieved_posts.posts) > 0:
                for post in retrieved_posts.posts:
                    posts.append(json.loads(post.model_dump_json()))
            # Debug only:
            # print(f"[+] Got {len(profile_feed.feed)} new posts for {user_screen_name} | Total = {len(feeds)}")

            # Debug
            # Printing call number every 500 calls
            if num_calls % 100 == 0 and num_calls > 0:
                print(f"[*] Call number: {num_calls} for {query}")


            # Updating the cursor
            cursor = retrieved_posts.cursor

            # Updating the condition
            condition = (cursor is not None) and (len(posts) < max_posts)

            if not condition:
                # If we have crawled no posts, we return None
                if len(posts) == 0:
                    return None

                # print(f"[+] Got {len(feeds)} posts for {user_screen_name}")

                # Otherwise, we return them
                return {query: posts}
            
            # Being polite
            time.sleep(TIME_SLEEP_POLITENESS)

        except Exception as e:
            # Printing the error
            print(f"[-] Something went wrong while getting the posts for {query}")
            print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
            return None
        

def get_profiles_from_query(at_client, query, limit=100):
    """
    Get profiles containing a given query
    :param at_client: Client object
    :param query: The query to search for
    :return: {query : list of handles}
    """
    # Preparing cursor
    cursor = None

    # Initializing the list of posts
    handles = []

    # Initially condition is true (i.e., current_size < max_posts or cursor will not be None)
    condition = True

    # Debug variables
    num_calls = 0

    # Get profile's posts using the cursor as a paginator, max_posts+eps (i.e., the whole call) posts will be retrieved
    while condition:
        try:
            # Before making the call, we check the rate limit
            check_rate_limits(at_client)

            # Preparing the parameters
            params = models.AppBskyActorSearchActors.Params(cursor=cursor, 
                                                            q=query, 
                                                            limit=limit)


            # Getting the author feed
            retrieved_profiles = at_client.app.bsky.actor.search_actors(params)


            # Appending the posts to the list
            if retrieved_profiles.actors is not None and len(retrieved_profiles.actors) > 0:
                for actor in retrieved_profiles.actors:
                    handles.append(actor.handle)
            # Debug only:
            # print(f"[+] Got {len(profile_feed.feed)} new posts for {user_screen_name} | Total = {len(feeds)}")

            # Debug
            # Printing call number every 500 calls
            if num_calls % 100 == 0 and num_calls > 0:
                print(f"[*] Call number: {num_calls} for {query}")


            # Updating the cursor
            cursor = retrieved_profiles.cursor

            # Updating the condition
            condition = cursor is not None

            if not condition:
                # If we have crawled no posts, we return None
                if len(handles) == 0:
                    return None

                # print(f"[+] Got {len(feeds)} posts for {user_screen_name}")

                # Otherwise, we return them
                return {query: handles}
            
            # Being polite
            time.sleep(TIME_SLEEP_POLITENESS)

        except Exception as e:
            # Printing the error
            print(f"[-] Something went wrong while getting the profiles for {query}")
            print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
            return None
        

def get_users_discussing_query(at_client, query, limit=100):
    """
    Get handles of users having posts containing a given query
    :param at_client: Client object
    :param query: The query to search for
    :return: {query: list of handles}
    """
    # Preparing cursor
    cursor = None

    # Initializing the list of posts
    handles = set()

    # Initially condition is true (i.e., current_size < max_posts or cursor will not be None)
    condition = True

    # Debug variables
    num_calls = 0

    # Get profile's posts using the cursor as a paginator, max_posts+eps (i.e., the whole call) posts will be retrieved
    while condition:
        try:
            # Before making the call, we check the rate limit
            check_rate_limits(at_client)

            # Preparing the parameters
            params = models.AppBskyFeedSearchPosts.Params(cursor=cursor, 
                                                          q=query, 
                                                          limit=limit)


            # Getting the author feed
            retrieved_posts = at_client.app.bsky.feed.search_posts(params)


            # Appending the posts to the list
            if retrieved_posts.posts is not None and len(retrieved_posts.posts) > 0:
                for post in retrieved_posts.posts:
                    handles.add(post.author.handle)
            # Debug only:
            # print(f"[+] Got {len(profile_feed.feed)} new posts for {user_screen_name} | Total = {len(feeds)}")

            # Debug
            # Printing call number every 500 calls
            if num_calls % 100 == 0 and num_calls > 0:
                print(f"[*] Call number: {num_calls} for {query}")


            # Updating the cursor
            cursor = retrieved_posts.cursor

            # Updating the condition
            condition = cursor is not None

            if not condition:
                # If we have crawled no posts, we return None
                if len(handles) == 0:
                    return None

                # print(f"[+] Got {len(feeds)} posts for {user_screen_name}")

                # Otherwise, we return them
                return {query: list(handles)}
            
            # Being polite
            time.sleep(TIME_SLEEP_POLITENESS)

        except Exception as e:
            # Printing the error
            print(f"[-] Something went wrong while getting the handles for {query}")
            print(f"[-] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
            return None