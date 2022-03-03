#!/usr/bin/env python3

import sys
import requests
import logging

log = logging.getLogger(__name__)

ES7_URL = "http://localhost:9200"
ES7_INDEX = "my-index"
NEW_ES7_INDEX = "my-index_2"
# please implement this


def in_gen():
    k = 0
    size = 10
    while True:
        resp = requests.get(f"{ES7_URL}/{ES7_INDEX}/_search?from={k}&size={size}")
        resp.raise_for_status()
        records = resp.json()['hits']['hits']
        if not records:
            break
        yield records
        k += size


def get_calculated(rec, calc_field_name):
    res = sum([
        len(k) + len(v) for k, v in rec.items()
        if type(v) == str and k != calc_field_name
    ])
    return res


def dispatch_gen(records):
    for chunk in records:
        for doc in chunk:
            _source = doc['_source']
            _source.update({
                'calculated': get_calculated(
                    rec=_source,
                    calc_field_name='calculated'
                )
            })
            resp = requests.post(f"{ES7_URL}/{NEW_ES7_INDEX}/_doc", json=_source)
            resp.raise_for_status()
            yield _source


def main():
    records = in_gen()
    pylines = dispatch_gen(records)
    for line in pylines:
        print(f'process {line}')


if __name__ == '__main__':
    main()
