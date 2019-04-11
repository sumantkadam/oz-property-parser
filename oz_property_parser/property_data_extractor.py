#!/usr/env/bin python3

"""Main program to run the property data extractor."""

import argparse
import csv
import logging
import os
import shutil
import zlib

from typing import List

import archive_mgr
import db_store
import property_file_manager as prop_mgr
import property_parser
import project_logger

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def parse_args() -> argparse.Namespace:
    """Set up command line arguments for Transdump."""
    parser = argparse.ArgumentParser()
    parser.add_argument('dir',
                        help='Base search Dir for property Files')
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """Validate the command line arguments."""
    if (not os.path.exists(args.dir)) or (not os.path.isdir(args.dir)):
        raise ValueError(F'"{args.dir}" is not a vaid directory')


def file_size(file_path):
    """Get the file size in bytes of the given file."""
    file_size = os.path.getsize(file_path)
    return file_size


def checksum_adler32(file_path):
    """Calculate the adler32 checksum of the given file."""
    csum = 1
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            csum = zlib.adler32(chunk, csum)
    csum = csum & 0xffffffff
    return csum


def add_scanned_file(sql_data_manager, file_path, size, checksum, extracted_from=None):
    """Add a scanned File."""
    scanned_file = db_store.ScannedFile(
        full_path=file_path, processed=False, size_bytes=size,
        checksum=checksum, extracted_from_id=extracted_from)
    sql_data_manager._session.add(scanned_file)
    sql_data_manager._session.flush()
    return scanned_file


def write_property_to_sql(sql_data_manager,
                          property_file: property_parser.PropertyFile) -> None:
    """Write the Property file data to SQL."""
    property_data = property_file.get_lines_as_list()

    sql_data_manager.add_property_list(property_data)


def get_csv_keys() -> List[str]:
    """Create a list of csv keys."""
    key_list = []
    ignore_keys: List[property_parser.PropertyData] = []

    for field in property_parser.PropertyData:
        if field not in ignore_keys:
            key_list.append(field.value)

    logger.debug(F'Created Key List: {key_list}')

    return key_list


def write_property_to_csv(csv_path: str,
                          property_file: property_parser.PropertyFile) -> None:
    """Write the parsed log file to a csv file."""
    write_header = not os.path.exists(csv_path)

    # Get the data to write (List of Dics to write)
    csv_data = property_file.get_lines_as_list()

    logger.info(F'Writing/Appending to: "{csv_path}"')
    with open(csv_path, 'a', encoding='utf-8') as csv_file:
        dict_writer = csv.DictWriter(csv_file, delimiter=',',
                                     lineterminator='\n',
                                     extrasaction='ignore',
                                     fieldnames=get_csv_keys())
        if write_header:
            logger.debug(F'Writing Header Row')
            dict_writer.writeheader()
        logger.debug(F'Writing {len(csv_data)} entries')
        dict_writer.writerows(csv_data)


def parse_path(sql_data_manager, path: str, csv_path: str, parent_file_id=None) -> None:
    """Parse the path for Property files."""
    logger.info(F'Parse Property files in "{path}", ParentFileId: "{parent_file_id}"')

    for root, _, files in os.walk(path):
        for filename in files:
            file_path = os.path.join(root, filename)
            logger.info(F'Process "{file_path}"')

            # Setup Scanned file in the DB
            size = file_size(file_path)
            checksum = checksum_adler32(file_path)
            db_file_entry = sql_data_manager._session.query(
                db_store.ScannedFile).filter_by(
                    size_bytes=size, checksum=checksum).first()
            if not db_file_entry:
                db_file_entry = add_scanned_file(
                    sql_data_manager, file_path, size, checksum, parent_file_id)

            # Don't process the file if done previously
            if db_file_entry.processed:
                logger.info(F'Skipping, File previously processed')
                continue

            # Check file for extraction
            if archive_mgr.file_is_archive(file_path):
                dest_dir = os.path.join(root, 'EXTRACT_' + filename)
                logger.info(F'Extracting "{file_path}" to "{dest_dir}"')
                try:
                    archive_mgr.extract(file_path, dest_dir)
                except archive_mgr.ExtractionError as error:
                    logger.exception('Extraction Error: "{error}"')
                else:
                    # Recursion - Check the Extracted folder for Files as well
                    parse_path(sql_data_manager, dest_dir, csv_path, db_file_entry.id)

                    # Commit for each archive to not delay too much
                    sql_data_manager._commit()

                    # Delete the created folder again
                    logger.debug(F'Deleting Extration directory "{dest_dir}"')
                    try:
                        shutil.rmtree(dest_dir)
                    except OSError as error:
                        logger.exception(
                            F'Deletion Failed to delete "{dest_dir}", Error: "{error}"')
                    else:
                        logger.debug('Deletion Succeeded')

            else:
                # Process the file as property file
                try:
                    property_file = prop_mgr.get_property_file_from_path(
                        file_path)
                except ValueError as error:
                    logger.error(F'Failed to Identify Property File: {error}')
                else:
                    logger.info('Parse Log File')
                    property_file.parse()
                    logger.info('Parsing complete')
                    '''
                    logger.info('Export to CSV')
                    write_property_to_csv(csv_path, property_file)
                    '''
                    logger.info('Export to SQL')
                    write_property_to_sql(sql_data_manager, property_file)
                    logger.info('Export complete')

            # Flag the File as Processed
            db_file_entry.processed = True


def main() -> None:
    """Run the log parser."""
    # Parse command line arguments
    args = parse_args()
    validate_args(args)

    # Setup the logger
    project_logger.setup_logger(
        os.path.join(args.dir, R'property_parser.log'))

    logger.info(F'Command Line Arguments: "{args}"')

    db_path = os.path.join(args.dir, F'ParseResult_Properties.sql')
    with db_store.SqliteDb(db_path) as db:
        if not os.path.exists(db_path):
            columns = [str(field.value) for field in property_parser.PropertyData]
            db.create(columns)

        with db.session_scope() as session:
            with db_store.DataManager(session, 1000000) as sql_data_manager:

                # Process Log Dir
                parse_path(
                    sql_data_manager, args.dir,
                    os.path.join(args.dir, F'ParseResult_Properties.csv'))


if __name__ == '__main__':
    main()
