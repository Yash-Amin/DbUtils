"""Main"""
import argparse

from dbutils import constants
from dbutils.actions import insert as insert_mode
from dbutils.actions import query as query_mode


def parse_arguments():
    """Returns parsed known arguments."""
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument(
        "mode",
        help="Operation mode",
        choices=[
            constants.Modes.CONVERT,
            constants.Modes.INSERT,
            constants.Modes.QUERY,
        ],
    )

    parser.add_argument("options", help="Options related to mode", nargs="*")

    known_args, _ = parser.parse_known_args()

    return known_args


def main():
    """Main method

    Parses arguments and runs given action.
    """
    args = parse_arguments()

    if args.mode == constants.Modes.INSERT:
        insert_mode.run(insert_mode.create_options_from_args())
    elif args.mode == constants.Modes.QUERY:
        query_mode.run(query_mode.create_options_from_args())
    elif args.mode == constants.Modes.CONVERT:
        pass


if __name__ == "__main__":
    main()
