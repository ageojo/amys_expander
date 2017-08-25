import csv
from functools import wraps
import itertools
from pprint import pprint
import os
import re
import time

import requests


OUTPUT_DIR = 'output'
DATA_DIR = 'data'
ORIG_FILE = os.path.join(DATA_DIR, 'shortened_links')


#############
# Token stuff
#############

def get_token_from_file():
    with open('bitly_token', 'r') as f:
        return f.read().strip()


def get_token():
    """
    Try in this order:
    1. BITLY_TOKEN env var
    2. bitly_token file in same dir as execution
    """
    return os.environ.get('BITLY_TOKEN') or get_token_from_file()


####################
# File Parsing Stuff
####################

def read_orig_file():
    with open(ORIG_FILE, 'r') as f:
        return f.readlines()


def is_bitly(line):
    return 'bit.ly' in line


def filter_bitly(lines):
    return [line for line in lines
            if is_bitly(line)]


def extract_hash(line):
    match = re.search(r'/(\w+),', line)
    return match.group(1)


def extract_hashes(lines):
    return [extract_hash(line) for line in lines]


def linkify(line):
    parts = line.split(',')[:3]
    return f'{parts[0]}://{parts[1]}{parts[2]}'


def linkify_lines(lines):
    return [linkify(line) for line in lines]


def csv_write(output_lines, outfile):
    out = os.path.join(OUTPUT_DIR, outfile)
    with open(out, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(output_lines)


#####################################
# Stuff that should be in the std lib
#####################################

def bucket(items, n):
    """
    Breaks items into a list of lists of n items each. Order is retained:

    >>> bucket([1, 2, 3, 4, 5, 6], 2)
    [[1, 2], [3, 4], [5, 6]]

    """

    bucket = []
    start = 0
    sub = items[start:start+n]
    while sub:
        bucket.append(sub)
        start += n
        sub = items[start:start+n]
    return bucket


def flatten_one_level(lists):
    return itertools.chain.from_iterable(lists)


#################
# Bitly API stuff
#################

def politeable(f):
    """
    Add a politeness argument, which is the number of seconds to sleep before
    calling the deforated function.

    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        politeness = kwargs.pop('politeness', 0)
        time.sleep(politeness)
        return f(*args, **kwargs)

    return wrapper


class Bitly():
    API_URL = 'https://api-ssl.bitly.com'
    EXPAND_ENDPOINT = 'v3/expand'
    BATCH_SIZE = 15  # https://dev.bitly.com/links.html#v3_expand
    POLITENESS = 1  # seconds

    def __init__(self, token):
        self.token = token

    def expand_one(self, bhash):
        url = f'{self.API_URL}/{self.EXPAND_ENDPOINT}'
        response = requests.get(url, {'access_token': self.token,
                                      'format': 'txt',
                                      'hash': bhash})
        return response.text.strip()

    def expand_all(self, hashes):
        batches = bucket(hashes, self.BATCH_SIZE)
        nested_hashes = [self._expand_batch(batch,
                                            politeness=self.POLITENESS)
                         for batch in batches]
        return flatten_one_level(nested_hashes)

    @politeable
    def _expand_batch(self, hashes, polite=False):
        url = f'{self.API_URL}/{self.EXPAND_ENDPOINT}'
        response = requests.get(url, {'access_token': self.token,
                                      'hash': hashes})
        return [item['long_url']
                for item in response.json()['data']['expand']]


######
# Main
######

if __name__ == '__main__':
    lines = read_orig_file()

    # Input file processing
    bitly_lines = filter_bitly(lines)
    bitly_links = linkify_lines(bitly_lines)
    bitly_hashes = extract_hashes(bitly_lines)

    # Call bitly API
    token = get_token()
    bitly = Bitly(token)
    urls = bitly.expand_all(bitly_hashes)

    # Output file processing
    output_lines = zip(bitly_lines, bitly_links, urls)
    pprint(list(output_lines))
    csv_write(output_lines, 'bitly_expansions.csv')
