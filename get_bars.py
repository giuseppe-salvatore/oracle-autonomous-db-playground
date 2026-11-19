import argparse
import sys
from datetime import datetime
from lib.database.factory import create_data_source
from lib.utils.cli import install_error_handler

available_db = ["OracleDB", "SQLite3DB"]

if __name__ == "__main__":
    install_error_handler("get_bars")
    parser = argparse.ArgumentParser(description="Fetch minute bars for a symbol.")
    parser.add_argument("db", choices=available_db)
    parser.add_argument("symbol")
    args = parser.parse_args()
    db, symbol = args.db, args.symbol

    if db in available_db:
        datasource = create_data_source(db)
        start = datetime.utcnow()
        ret = datasource.get_raw_data_for_symbol(symbol)
        stop = datetime.utcnow()
        fetch_data = "Data for {} has {} rows, fetched in {}".format(
            symbol, len(ret), stop - start
        )
        start = datetime.utcnow()
        format_str = "{} | {} | {:.2f} | {:.2f} | {:.2f} | {:.2f} | {}\n"
        for el in ret:
            sys.stdout.write(
                format_str.format(
                    el[1],
                    str(el[2]),
                    el[3],
                    el[4],
                    el[5],
                    el[6],
                    el[7],
                )
            )
        sys.stdout.flush()
        stop = datetime.utcnow()
        print_data = "Data for {} printed in {}".format(symbol, stop - start)
        print(fetch_data)
        print(print_data)
    else:
        raise Exception(f"Unknown database {db}")
