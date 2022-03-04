#!/usr/bin/env python3
import argparse
import json
import requests
import logging
import sys

log = logging.getLogger(__name__)

# ES7_URL = "http://localhost:9200"
ES7_INDEX = "index1"
# NEW_ES7_INDEX = "index2"
# please implement this


def in_gen(es_url, es_index):

    log.info(f'cleaning new index {es_index}')
    out = requests.delete(f"{es_url}/{es_index}")
    log.info(f"URL:{es_url}/{es_index}")
    log.info(out)
    log.info(out.json())
    log.info("DONE")

    payload = {
        "size": 100,
    }
    init_resp = requests.post(
        f"{es_url}/{ES7_INDEX}/_search?scroll=1m",
        json=payload,
    )
    init_resp.raise_for_status()
    j_resp = init_resp.json()
    records = j_resp['hits']['hits']
    _scroll_id = j_resp['_scroll_id']

    while records:
        log.info(f'GET RECORDS {len(records)}')
        yield records
        payload = {
            'scroll': '1m',
            'scroll_id': _scroll_id
        }
        resp = requests.post(
            f"{es_url}/_search/scroll",
            json=payload
        )
        resp.raise_for_status()
        j_resp = resp.json()
        records = j_resp['hits']['hits']
        _scroll_id = j_resp['_scroll_id']


def get_calculated(rec, calc_field_name):
    # can be optimized
    res = sum([
        len(k) + len(v) for k, v in rec.items()
        if type(v) == str and k != calc_field_name
    ])
    return res


def dispatch_gen(records, es_url, new_es_index):
    headers = {'Content-Type': 'application/x-ndjson'}
    for chunk in records:
        payload = []
        for doc in chunk:
            _source = doc['_source']
            _source.update({
                'calculated': get_calculated(
                    rec=_source,
                    calc_field_name='calculated'
                )
            })
            # can be optimized
            str1 = json.dumps({'index': {'_index': new_es_index, '_type': '_doc'}})
            str2 = json.dumps(_source)
            payload.append('%s\n%s\n' % (str1, str2))

        resp = requests.post(
            f"{es_url}/_bulk",
            data="".join(payload),
            headers=headers
        )
        resp.raise_for_status()
        j_resp = resp.json()
        log.info(f'MODIFIED {j_resp}')
        yield j_resp['items']


def main(args=sys.argv[1:]):
    logging.basicConfig(handlers=[logging.StreamHandler()])
    log.setLevel(logging.INFO)
    ap = argparse.ArgumentParser(description="Converter")
    ap.add_argument('--elasticsearch-url', type=str)
    ap.add_argument('--index', type=str)
    args = ap.parse_args(args)
    log.info("parameters in converter: %s", args)
    records = in_gen(args.elasticsearch_url, args.index)
    pipe = dispatch_gen(records, args.elasticsearch_url, args.index)
    i = 0
    for line in pipe:
        log.info(f'process {line}')
        i += 1


if __name__ == '__main__':
    main()
