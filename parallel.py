"""
Measure the influence of twitter users in parrallel
"""
import threading
import logging
import sys
import time
import json
import math
import codecs


data_file = 'sample_data.json'
max_n_threads = 12

tweets = []

def worker(i,n_threads,):
    """
    thread worker function
    """
    logging.debug('starting')
    # thread safe operations
    #time.sleep(3)
    # calculate the start_pos and end_pos this thread takes care of
    batch_size = len(tweets)//n_threads
    start_pos = i * batch_size
    end_pos = start_pos + batch_size - 1
    if i == (n_threads - 1):
        end_pos = len(tweets) - 1
    
    logging.debug('processing batch {}: tweets[{}] to tweets[{}]'.format(i,start_pos,end_pos))
    # the main calculation process
    for tweet in tweets:
        calculate_influence(tweet['followers_count'],tweet['retweet_count'])
    logging.debug('ending')

    
def calculate_influence(followers_count, retweet_count):
    """influence = popularity_level * follower_engagement_level
    the distribution of followers_count and retweet_count are highly skewed,
    take the log of them to get the level information
    """
    popularity_level = math.log10(followers_count)
    follower_engagement_level = math.log10(retweet_count)
    influence = popularity_level * follower_engagement_level
    return influence


logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
)

if __name__ == '__main__':
    # read in data file
    with codecs.open(data_file, 'r', 'utf-8') as f:
        for line in f:
            tweets.append(json.loads(line))
    logging.debug('Read in {} tweets'.format(len(tweets)))
    
    # receive threads number, default is 1
    if len(sys.argv) > 1:
      n_threads = int(sys.argv[1])
    else:
      n_threads = 1
    
    # max number of threads is 12
    if n_threads > max_n_threads:
        logging.debug('Max {} threads allowed!'.format(max_n_threads))
        n_threads = max_n_threads
    
    # start multiple threads as requested
    logging.debug('Starting {} threads'.format(n_threads))
    for i in range(n_threads):
        t = threading.Thread(target=worker, args=(i,n_threads,))
        t.start()
    
    # join all threads except the main thread
    main_thread = threading.main_thread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        t.join()
    logging.debug('Joined threads')
    logging.debug('{} thread remaining'.format(threading.active_count()))
