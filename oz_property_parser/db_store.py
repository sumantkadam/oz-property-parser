#!/usr/bin/env python3

"""Manage the Database."""

import logging

from contextlib import contextmanager

from sqlalchemy import (Boolean, Column, Integer, String, ForeignKey, Table,
                        UniqueConstraint, create_engine, Unicode, MetaData)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.interfaces import PoolListener

from sqlalchemy.orm import relationship, sessionmaker, backref, mapper

Base = declarative_base()

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ScannedFile(Base):
    """Represent the already scanned files."""

    __tablename__ = 'scanned_file'

    # Use ID, keeps the foreign key size smalle
    id = Column(Integer, primary_key=True)
    full_path = Column(String)
    processed = Column(Boolean)
    size_bytes = Column(String)
    checksum = Column(String)

    extracted_from_id = Column(Integer, ForeignKey('scanned_file.id'))
    extracted_from = relationship("ScannedFile", remote_side=[id])

    UniqueConstraint('size_bytes', 'checksum', name='uix_1')


class SalesData():
    """Sales Data DB."""


class SqliteForeignKeysListener(PoolListener):
    """Class to setup Foreign Keys."""
    def connect(self, dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON')


class SqliteDb():
    """SQLAlchemy Sqlite database connection."""

    def __init__(self, db_path):
        self.connection_string = 'sqlite:///' + db_path

    def __enter__(self):
        self.engine = create_engine(
            self.connection_string,
            #echo=True,
            listeners=[SqliteForeignKeysListener()])  # Enforce Foreign Keys

        self.Session = sessionmaker(bind=self.engine)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create(self, sales_data_columns=[]):

        ### ERIC TEST START - Think how to get the list here nicer without depending of property_parser
        #import property_parser
        sales_table = Table('SalesData', Base.metadata, Column('id', Integer, primary_key=True),
                  *(Column(col, Unicode(255)) for col in sales_data_columns))
        mapper(SalesData, sales_table)
        ### ERIC TEST END

        Base.metadata.create_all(self.engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


class DataManager():
    """Manager for combined commits."""

    def __init__(self, session, commit_max=10000):
        logger.info('DataManager.__init__()')
        self._commit_max = commit_max
        self._property_count = 0
        self._property_list = []
        self._session = session
        self._commit_count = 0
        self._property_total = 0

    def __enter__(self):
        logger.info('DataManager.__enter__()')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._commit()
        logger.info(F'  Exit: Adds: {self._property_total}, Commits: {self._commit_count}')

    def add_property_list(self, property_list):
        count = len(property_list)
        self._property_list += property_list
        self._property_count += count
        self._property_total += count

        if self._property_count >= self._commit_max:
            self._commit()

    def _commit(self):
        logger.info('DataManager._commit()')
        if self._property_count > 0:
            logger.info(F'DataManager._commit(): Property Count: {self._property_count}')
            #logger.debug(F'{datetime.datetime.now()} - Insert Start')
            insert_bulk_sales_data(self._session, self._property_list)
            #logger.debug(F'{datetime.datetime.now()} - Insert End, Commit Start')
            self._session.commit()
            #logger.debug(F'{datetime.datetime.now()} - Commit End')
            self._property_count = 0
            self._commit_count += 1
            del self._property_list[:]
            logger.info(F'  Adds: {self._property_total:20}, Commits: {self._commit_count:10}')


def insert_bulk_sales_data(session, data_dic):
    session.bulk_insert_mappings(SalesData, data_dic)


def test():
    import os
    import collections
    db_path = R'C:\temp\tmp\DbTest\test.db'
    with SqliteDb(db_path) as db:
        if not os.path.exists(db_path):
            db.create()

        with db.session_scope() as session:
            data_list = [
                {'HouseNumber': '15', 'StreetName': 'Ring','PostCode': '2211'},
                {'HouseNumber': 'U400/18', 'StreetName': 'Ring','PostCode': '2211'},
                {'HouseNumber': '111', 'StreetName': 'Ring 2','PostCode': '3456'},
            ]
            data_list = [
                collections.defaultdict(lambda: '', {'HouseNumber': '15', 'StreetName': 'Ring','PostCode': '2211'}),
                collections.defaultdict(lambda: '', {'HouseNumber': 'U400/18', 'StreetName': 'Ring','PostCode': '2211'}),
                collections.defaultdict(lambda: '', {'HouseNumber': '111', 'StreetName': 'Ring 2','PostCode': '3456'}),
            ]
            insert_bulk_sales_data(session, data_list)

if __name__ == '__main__':
    test()
