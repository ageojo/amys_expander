import csv
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
    with open(out, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(output_lines)


#################
# Bitly API stuff
#################

class Bitly():
    API_URL = 'https://api-ssl.bitly.com'
    EXPAND_ENDPOINT = 'v3/expand'

    def __init__(self, token):
        self.token = token

    def expand(self, bhash):
        url = f'{self.API_URL}/{self.EXPAND_ENDPOINT}'
        response = requests.get(url, {'access_token': self.token,
                                      'format': 'txt',
                                      'hash': bhash})
        expansion = response.text.strip()
        pprint(expansion)
        return expansion

    def _polite_expand(self, bhash, secs=1):
        time.sleep(secs)
        return self.expand(bhash)

    def expand_multiple(self, hashes):
        return [self._polite_expand(bhash) for bhash in hashes]


if __name__ == '__main__':
    lines = read_orig_file()

    bitly_lines = filter_bitly(lines)
    bitly_links = linkify_lines(bitly_lines)
    bitly_hashes = extract_hashes(bitly_lines)

    token = get_token()
    bitly = Bitly(token)
    urls = bitly.expand_multiple(bitly_hashes)
    output_lines = zip(bitly_lines, bitly_links, urls)
    pprint(output_lines)
    csv_write(output_lines, 'bitly_expansions')
