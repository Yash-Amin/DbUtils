"""Insert mode"""
import os
import re
import argparse
from pymongo import mongo_client
from dataclasses import dataclass
from typing import Dict, List, Pattern
from pymongo.mongo_client import MongoClient

from dbutils import constants
from dbutils.utils import get_comma_separated_fields, str2bool


@dataclass
class QueryModeOptions:
    # Database name
    database: str
    # Collection name
    collection: str
    # Specify comma-separeted columns names, given fields will be projected
    # If no value is specified, all columns will be returned in the output
    columns: List[str]
    # Limit number of records
    limit: int
    # provide batch size for file-chunks mode
    # script will also use batch_size to fetch record in batches
    batch_size: int
    # Specify output mode (stdout, file, fie-chunks)
    output_mode: str
    # Specify output file type (json, csv)
    output_file_type: str
    # Specify bool to iclude header in csv file format
    include_header: bool
    # Output path. For file-chunks mode provide dir path, for file mode provide
    # file path
    output_path: str
    # For file-chunks mode, provide file prefix
    output_file_prefix: str
    # For file-chunks mode, provide file extension
    output_file_extension: str
    # Provide queries
    queries: Dict[str, Pattern]
    # MongoDB client
    mongodb_client: MongoClient
    # MongoDB collection
    mongodb_collection: any
    # default mode
    mode: str = constants.Modes.QUERY


def parse_arugments() -> argparse.Namespace:
    """Parse arguments for query mode."""
    parser = argparse.ArgumentParser(description="DbUtils - query mode")
    parser.add_argument("mode", help="Enter mode", choices=[constants.Modes.QUERY])

    parser.add_argument("-database", help="Mongodb Database Name", required=True)
    parser.add_argument("-collection", help="Mongodb Collection Name", required=True)
    parser.add_argument(
        "-columns",
        help="Given comma-separated values will be projected in the output (default=all columns)",
        default="",
    )
    parser.add_argument("-batch-size", help="Batch size", default=500, type=int)
    parser.add_argument("-limit", help="Limit number of records.", default=-1, type=int)
    parser.add_argument(
        "-output-mode",
        help="Output modes. stdout=output on stdout, file=write output to file, file-chunks=write output in chunks (create multiple smaller files, use -batch-size arguments to customize batch-size.)",
        required=True,
        choices=[
            constants.OutputMode.FILE,
            constants.OutputMode.FILE_CHUNKS,
            constants.OutputMode.STDOUT,
        ],
    )
    parser.add_argument(
        "-output-file-type",
        help="Output file type",
        required=True,
        choices=[
            constants.FileTypes.CSV,
            constants.FileTypes.JSON,
        ],
    )
    parser.add_argument(
        "-include-header",
        help="Include header for CSV file",
        default=False,
        type=str2bool,
    )
    parser.add_argument(
        "-output-path",
        help="If output-mode is file, provide file name. If output-mode is file-chunks, provide directory name",
        default="",
    )
    parser.add_argument(
        "-output-file-prefix",
        help="Output file prefix for file-chunks mode",
        default="",
    )
    parser.add_argument(
        "-output-file-extension",
        help="Output file extension for file-chunks mode",
        default="txt",
    )

    parser.add_argument(
        "-queries",
        help="Provide regex queries in this format - '-queries KEY_NAME_1=REGEX_1 KEY_NAME_2=REGEX-2'",
        nargs="*",
    )

    args = parser.parse_args()

    # output-path is required when output-mode is file or file-chunks
    if args.output_mode != constants.OutputMode.STDOUT and args.output_path == "":
        parser.print_usage()

        raise argparse.ArgumentTypeError(
            "output-path is required when output-mode is file or file-chunks."
        )

    # output-file-prefix is required when output-mode is file-chunks
    if (
        args.output_mode == constants.OutputMode.FILE_CHUNKS
        and args.output_file_prefix == ""
    ):
        parser.print_usage()

        raise argparse.ArgumentTypeError(
            "output-file-prefix is required when output-mode is file-chunks."
        )

    args.queries = {
        query[: query.index("=")]: re.compile(query[query.index("=") + 1 :])
        for query in args.queries or []
    }

    args.columns = get_comma_separated_fields(args.columns)

    return args


def create_options_from_args() -> QueryModeOptions:
    """Parses arguments and returns InsertModeOptions."""
    args = vars(parse_arugments())

    # FIXME: get mongodb connection string from environment variable
    mongo_client = MongoClient()

    args["mongodb_client"] = mongo_client
    args["mongodb_collection"] = mongo_client[args["database"]][args["collection"]]

    return QueryModeOptions(**args)


def run(options: QueryModeOptions) -> None:
    """Runs query mode."""

    if options.output_mode == constants.OutputMode.FILE_CHUNKS:
        # Creates directory for file-chunks mode
        os.makedirs(options.output_mode, exist_ok=True)
    elif options.output_mode == constants.OutputMode.FILE:
        # If output-mode is `file` and if output-path is '/some/path/file.csv'
        # and if directory '/some/path/' does not exist, it will be created
        dir_path = os.path.dirname(os.path.abspath(options.output_path))
        os.makedirs(dir_path, exist_ok=True)

    db = options.mongodb_collection

    last_id = ""
    current_batch = 0

    # Fetch records in batches
    records = db.find(options.queries).limit(options.batch_size)

    while records:
        # Records to output will be stored in this list
        output_records = []

        for record in records:
            last_id = record["_id"]

            # Create dict containing the fields specified using 'columns' argument
            output_record = {
                key: value
                for key, value in record.items()
                if len(options.columns) == 0 or key in options.columns
            }

            output_records.append(output_record)

        # If no records are found, exit
        if len(output_records) == 0:
            return

        current_batch += 1

        # Fetch record for the next batch
        next_query = {**options.queries, "_id": {"$gt": last_id}}
        records = db.find(next_query).limit(options.batch_size)

        # If number of fetched records is >= limit provided, exit
        if options.limit > 0 and current_batch * options.batch_size >= options.limit:
            return
