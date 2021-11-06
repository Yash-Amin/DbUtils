"""Insert mode"""
import json
import argparse
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass
from pymongo.mongo_client import MongoClient

from dbutils import constants
from dbutils.utils import str2bool


@dataclass
class InsertModeOptions:
    # ID field for records, provide custom ID field to match else use None
    id_field: str
    # Database name
    database: str
    # Collection name
    collection: str
    # If create_or_update is True, then if old record is found it will,
    # be updated with new values else new record will be inserted.
    create_or_update: bool
    # Input file path
    input_file: str
    # If true, the script will manage created_at and updated_at
    auto_manage_timestamps: bool
    # Provide list of fields to compare, if list is empty then all
    # fields except created_at and updated_at will be checked for
    # comparision.
    compare_fields: List[str]
    # Provide fields to ignore while comparision
    compare_ignore_fields: List[str]
    # MongoDB client
    mongodb_client: MongoClient
    # MongoDB collection
    mongodb_collection: any
    # default mode
    mode: str = "insert"


def parse_arugments() -> None:
    """Parse arguments for insert mode."""
    parser = argparse.ArgumentParser(description="DbUtils - insert mode")

    parser.add_argument(
        "mode",
        help="Enter mode",
        choices=[constants.Modes.INSERT],
    )
    parser.add_argument("-database", help="Mongodb Database Name", required=True)
    parser.add_argument("-collection", help="Mongodb Collection Name", required=True)
    parser.add_argument("-id-field", help="ID field used for collection", default="_id")
    parser.add_argument(
        "-auto-manage-timestamps",
        default=True,
        type=str2bool,
        help="If enabled, script will manage created_at, updated_at fields. (Default=true)",
    )
    parser.add_argument(
        "-create-or-update",
        help="By providing this flag, if record with _id exists, it will be updated with new values.",
        action="store_true",
        default=True,
    )
    parser.add_argument("-input-file", help="Input file path", required=True)
    parser.add_argument(
        "-compare-fields",
        help="Comma-separated field names for comparision. (Default=[All fields])",
        default="",
    )
    parser.add_argument(
        "-compare-ignore-fields",
        help="Comma-separated field names to ignore for comparision. (Default=None)",
        default="",
    )

    return parser.parse_args()


def create_options_from_args() -> InsertModeOptions:
    """Parses arguments and returns InsertModeOptions."""
    args = parse_arugments()

    # FIXME: get mongodb connection string from environment variable
    mongo_client = MongoClient()

    return InsertModeOptions(
        **vars(args),
        mongodb_client=mongo_client,
        mongodb_collection=mongo_client[args.database][args.collection],
    )


def insert_record(options: InsertModeOptions, record: Dict) -> None:
    """Inserts record in database."""
    id_field = options.id_field
    print(record)

    # TODO: check if old record exists
    old_record_found = False

    if options.create_or_update:
        # If some values of old record differs from new record
        # update values

        pass

    if not old_record_found:
        # If old record with given id does not exists, inserts it
        if options.auto_manage_timestamps:
            record["created_at"] = datetime.now()

        options.mongodb_collection.insert_one(record)

    elif not options.create_or_update:
        # If old record is found, and create_or_update option is off,
        # raise exception
        raise Exception(f"Record with {record.get(id_field, f'#{id_field}')=} exists")


def run(options: InsertModeOptions) -> None:
    """Runs insert mode."""

    with open(options.input_file, "r") as input_file:
        # TODO:
        #    - Add an argument to specify input file type like json,csv
        records = [
            json.loads(line)
            for line in input_file.read().splitlines()
            if line.strip() != ""
        ]

        for record in records:
            try:
                insert_record(options, record)
            except:
                # FIXME: add logger
                pass
