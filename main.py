import argparse
from src import organizer, metadata_csv

def main():
    parser = argparse.ArgumentParser(description="HUD Local Pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("extract", help="Extract and reorganize zips from archive/ to raw/")
    sub.add_parser("backfill", help="Generate CSV with metadata from raw/ to outputs/")

    args = parser.parse_args()

    if args.command == "extract": # python main.py extract
        organizer.run()
    elif args.command == "backfill": # python main.py backfill
        metadata_csv.run()

if __name__ == "__main__":
    main()
