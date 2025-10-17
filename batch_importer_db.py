# file: batch_importer_db.py

import argparse
import sys
import time
from datetime import datetime, timedelta
import re
from jesse.research import import_candles
from jesse.enums import exchanges
import jesse.helpers as jh
import psycopg2
from dbconfig import dbcfg

# # # # # # # # # # # # CONFIG # # # # # # # # # # # #
EXCHANGE = exchanges.BINANCE_PERPETUAL_FUTURES
START_DATE = "2018-01-01"  # Start date in YYYY-MM-DD format.
# # # # # # # # # # # # END CONFIG # # # # # # # # # # # #


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import candles for all symbols in DB, with optional scheduling."
    )
    parser.add_argument(
        "--now",
        action="store_true",
        help="Run the main job immediately (skip the initial wait).",
    )
    parser.add_argument(
        "--time",
        metavar="HH:MM",
        help="Local time (24h) to run each day, e.g. 23:00. "
             "If provided, the script sleeps until the next occurrence after each run.",
    )
    args = parser.parse_args()

    # Validate --time if provided
    if args.time:
        try:
            datetime.strptime(args.time, "%H:%M")
        except ValueError:
            log("Error: --time must be in HH:MM (24-hour) format, e.g. 23:00", file=sys.stderr)
            sys.exit(2)

    return args

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_symbols_from_db():
    conn = psycopg2.connect(**dbcfg)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT candle.exchange, candle.symbol from candle group by candle.exchange, candle.symbol;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(exchange, jh.dashy_symbol(symbol)) for exchange, symbol in rows]

def map_db_exchange_to_enum(name: str):
    """
    Convert a DB exchange string like 'Binance Perpetual Futures'
    into the corresponding jesse.enums.exchanges member.
    """
    # Normalize by stripping non-alphanumerics and uppercasing
    def _norm(s: str) -> str:
        return re.sub(r'[^A-Za-z0-9]+', '', s).upper()

    key = _norm(name)

    # Build a lookup of normalized enum names → actual enum name
    enum_names = [a for a in dir(exchanges) if a.isupper()]
    norm_to_attr = {_norm(a): a for a in enum_names}

    # 1) Direct match (handles spacing/underscore differences)
    if key in norm_to_attr:
        return getattr(exchanges, norm_to_attr[key])
    
    return None  # Not found

def fetch_candles(exchange, symbol, start_date):
    # Inform the user which symbol is being imported
    log(f"=== Importing candles for {symbol} ({exchange}) ===")
    try:
        import_candles(exchange, symbol, start_date, show_progressbar=False)
        log(f"=== Finished importing candles for {symbol} ({exchange})===\n")
        return True
    except ConnectionError as e:
        if '429' in str(e):
            log("Rate limit exceeded. Waiting for 1 minute.")
            time.sleep(60)  # Wait for 1 minute before retrying
        else:
            log("Network is down. Retrying in 5 minutes.")
            time.sleep(300)  # Wait for 5 minutes before retrying
        return False


def seconds_until_next(time_str: str) -> float:
    """
    Return seconds until the next occurrence of local HH:MM (today or tomorrow).
    """
    now = datetime.now()
    hh, mm = map(int, time_str.split(":"))
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def sleep_until(time_str: str):
    secs = seconds_until_next(time_str)
    next_at = datetime.now() + timedelta(seconds=secs)
    log(f"Sleeping until {next_at.strftime('%Y-%m-%d %H:%M:%S')} (local) "
          f"— {int(secs)} seconds.")
    time.sleep(secs)


def main():
    args = parse_args()

    # Initial wait logic
    if args.time and not args.now:
        # Wait until the specified time before first run
        print(f"--time {args.time} provided and --now not set: waiting for the first run time.")
        sleep_until(args.time)
    else:
        if args.now:
            log("--now provided: starting immediately.")
        else:
            log("No --time provided: will run now and then sleep 24 hours between runs.")

    try:

        while True:
            # Run the job
            symbols = get_symbols_from_db()
            log(f"Loaded {len(symbols)} symbols from DB.")
            for exch_name, symbol in symbols:
                # Map DB exchange string to Jesse enum
                exch_enum = map_db_exchange_to_enum(exch_name)
                if exch_enum is None:
                    log(f"Warning: unknown exchange '{exch_name}' for {symbol}, skipping.")
                    continue

                success = False
                while not success:
                    success = fetch_candles(exch_enum, symbol, START_DATE)
                    if not success:
                        time.sleep(5)  # Wait for 5 seconds before retrying

            # Post-run sleep
            now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log("Completed fetching candles for all symbols.")

            if args.time:
                # Sleep until next specified clock time
                sleep_until(args.time)
            else:
                # Default: sleep for 24 hours
                log("Sleeping for 24 hours (no --time specified).")
                time.sleep(86400)

    except KeyboardInterrupt:
        log("\nReceived KeyboardInterrupt, exiting gracefully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
