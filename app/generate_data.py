#!/usr/bin/env python3

import sys
import logging
import argparse


import requests


log = logging.getLogger(__name__)


def generate_docs(elasticsearch_url, index, size):
    log.info("cleaning old index...")
    out = requests.delete(f"{elasticsearch_url}/{index}")
    log.info(out)
    log.info(out.json())
    log.info("OK")

    log.info("creaing new index...")
    out = requests.put(f"{elasticsearch_url}/{index}")
    out.raise_for_status()
    log.info(out)
    log.info(out.json())
    log.info("OK")

    log.info("generating documents...")
    for _ in range(size):
        resp = requests.post(f"{elasticsearch_url}/{index}/_doc", json=dict(
            document_number=str(_),
            calculated=21
            ))
        resp.raise_for_status()
    log.info("documents generated.")

def main(args=sys.argv[1:]):
    logging.basicConfig(handlers=[logging.StreamHandler()])
    log.setLevel(logging.INFO)
    ap = argparse.ArgumentParser(description="Recreates index (delete and create) and push data into it")
    ap.add_argument('--elasticsearch-url', type=str)
    ap.add_argument('--index', type=str)
    args = ap.parse_args(args)
    log.info("parameters: %s", args)

    generate_docs(args.elasticsearch_url, args.index, 100000)




if __name__ == '__main__':
    main()

