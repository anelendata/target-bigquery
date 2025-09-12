#!/usr/bin/env python3

import argparse, logging
import simplejson as json

import singer

from target_bigquery.sync import sync
from target_bigquery.schema import modify_schema


LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
logger = singer.get_logger()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--schema', help='Automatically detect and add new BigQuery columns from catalog file', required=False)
    parser.add_argument('-d', '--dryrun', type=bool, help='dryrun mode (no write)', default=False, required=False)
    parser.add_argument('-i', '--continue-on-incompatible', type=bool, help='Continue the update by ignoring the incompatible columns', default=False, required=False)
    parser.add_argument('-t', '--tables', help='Comma-separated table names to update schema', default=False, required=False)
    parser.add_argument('-l', '--loglevel', help='Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)', default='INFO')
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    if args.loglevel:
        log_level = LOG_LEVELS.get(args.loglevel.upper())
        if not log_level:
            raise (f"Log level must be one of {','.join(LOG_LEVELS)}")
        logger.setLevel(log_level)

    numeric_type = config.get("numeric_type", "NUMERIC")
    integer_type = config.get("integer_type", "INTEGER")
    if args.schema:
        return modify_schema(
            config,
            args.schema,
            streams=args.tables,
            numeric_type=numeric_type,
            integer_type=integer_type,
            dryrun=args.dryrun,
            continue_on_incompatible=args.continue_on_incompatible,
        )
    sync(config)

if __name__ == '__main__':
    main()
