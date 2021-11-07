"""Query mode"""
import os
import re
import csv
import sys
import argparse
from bson import json_util
from dataclasses import dataclass
from pymongo.mongo_client import MongoClient
from typing import Dict, List, Pattern, TextIO

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
    parser = argparse.ArgumentParser(description="DbUtils - Query mode")
    parser.add_argument("mode", help="Operation mode", choices=[constants.Modes.QUERY])

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
        help=(
            "'stdout' output mode will print output in stdout. "
            "'file' output mode will write output to a file. "
            "'file-chunks' output mode will write output in smaller file chunks. "
            "Use batch-mode argument to specify batch size."
        ),
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
    """Parses arguments and returns QueryModeOptions."""
    args = vars(parse_arugments())

    # FIXME: get mongodb connection string from environment variable
    mongo_client = MongoClient()

    args["mongodb_client"] = mongo_client
    args["mongodb_collection"] = mongo_client[args["database"]][args["collection"]]

    return QueryModeOptions(**args)


def _write_json(records: List[Dict], output_file: TextIO):
    """Write records to json file."""
    # Using json_util.dumps convert mongodb record with object_id, to string
    # and write records to file
    output_file.writelines([json_util.dumps(record) + "\n" for record in records])

    # Close output stream
    if output_file != sys.stdout:
        # If output_file mode is stdout, closing it will cause error for print()
        output_file.close()


def _write_csv(records: List[Dict], output_file: TextIO, write_header: bool = False):
    """Write output to csv file."""
    # Find unique column names
    columns = set()
    for record in records:
        columns.update(record.keys())

    csv_writer = csv.DictWriter(output_file, fieldnames=sorted(columns))

    # Write columns
    write_header and csv_writer.writeheader()

    for record in records:
        csv_writer.writerow(record)

    # Close output stream
    if output_file != sys.stdout:
        # If output_file mode is stdout, closing it will cause error for print()
        output_file.close()


def output(options: QueryModeOptions, batch_id: int, records: List[Dict]):
    """Write output"""
    output_mode = options.output_mode

    if output_mode == constants.OutputMode.STDOUT:
        output_path = ""
        output_stream = sys.stdout

    elif output_mode == constants.OutputMode.FILE:
        output_path = options.output_path
        output_stream = open(output_path, "a")

    elif output_mode == constants.OutputMode.FILE_CHUNKS:
        output_file_name = (
            f"{options.output_file_prefix}-{batch_id}.{options.output_file_extension}"
        )

        output_path = os.path.join(options.output_path, output_file_name)
        output_stream = open(output_path, "w")

    # Write output to csv/json file
    if options.output_file_type == constants.FileTypes.CSV:
        _write_csv(records, output_stream, options.include_header)

    elif options.output_file_type == constants.FileTypes.JSON:
        _write_json(records, output_stream)


def run(options: QueryModeOptions) -> None:
    """Runs query mode."""

    if options.output_mode == constants.OutputMode.FILE_CHUNKS:
        # Creates directory for file-chunks mode
        os.makedirs(options.output_path, exist_ok=True)
    elif options.output_mode == constants.OutputMode.FILE:
        # If output-mode is `file` and if output-path is '/some/path/file.csv'
        # and if directory '/some/path/' does not exist, it will be created
        dir_path = os.path.dirname(os.path.abspath(options.output_path))
        os.makedirs(dir_path, exist_ok=True)

        # TODO: raise error if output-path exists, add new argument to
        # overwrite file if it exists.

        # If output_mode is file and output-path exists, this will delete it
        if os.path.exists(options.output_path) and os.path.isfile(options.output_path):
            os.unlink(options.output_path)
    elif options.output_mode == constants.OutputMode.FILE_CHUNKS:
        # TODO: if files with file-name matching output-mode-prefix and output-mode-extension
        # exists, raise error
        pass

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

        output(options, current_batch, output_records)

        current_batch += 1

        # Fetch record for the next batch
        next_query = {**options.queries, "_id": {"$gt": last_id}}
        records = db.find(next_query).limit(options.batch_size)

        # If number of fetched records is >= limit provided, exit
        if options.limit > 0 and current_batch * options.batch_size >= options.limit:
            return
