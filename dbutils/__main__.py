"""Main"""
import argparse

from dbutils import constants


def parse_arguments():
    """Returns parsed known arguments."""
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument(
        "mode",
        help="Enter mode",
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
        pass
    elif args.mode == constants.Modes.QUERY:
        pass
    elif args.mode == constants.Modes.CONVERT:
        pass


if __name__ == "__main__":
    main()
