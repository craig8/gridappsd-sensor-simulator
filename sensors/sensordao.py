import sqlite3
from queue import Queue, Empty
import logging
import threading
import time

_log = logging.getLogger(__name__)


class SensorDao(object):

    def __init__(self, dbpath):
        self._dbpath = dbpath
        self._measurements = {}
        self._conn = None
        self._batch = []
        self._queue = Queue()
        self._complete = False
        self._bg_thread = threading.Thread(target=self._background_queue, args=(self._queue, self._complete),
                                           daemon=True)
        self._bg_thread.start()

    def join_thread(self):
        self._complete = True
        self._bg_thread.join()

    def _background_queue(self, queue: Queue, complete: bool):
        _log.debug("Starting background queue")
        conn = self._create_connection(True)
        while not complete:
            try:
                batch = queue.get(timeout=5)
                _log.debug("inserting background queue")
                self._insert_batch(batch, conn)
            except Empty:
                # just go on and block on this thread until next time.
                pass
            time.sleep(0.1)

    def _create_connection(self, new=False):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None

        """
        if new:
            conn = None
            try:
                conn = sqlite3.connect(self._dbpath)
            except sqlite3.Error as e:
                _log.exception(e)
            return conn

        if self._conn is None:
            try:
                self._conn = sqlite3.connect(self._dbpath)
                self._create_tables()
            except sqlite3.Error as e:
                _log.exception(e)

    def _create_tables(self):
        sql = """
        CREATE TABLE IF NOT EXISTS measurement (
            measurement_mrid text primary key        
        );
        CREATE TABLE IF NOT EXISTS record (
            measurement_mrid text,
            property text,
            ts integer,
            original text,
            sensed text,
            FOREIGN KEY(measurement_mrid) REFERENCES measurement(measurement_mrid)
        );
        """
        self._create_connection()

        cur = None
        try:
            for d in sql.split(";"):
                if d.strip():
                    cur = self._conn.cursor()
                    cur.execute(d)
                    self._conn.commit()
                    cur.close()

        except sqlite3.Error as e:
            _log.exception(e)

    def _insert_measurement(self, measurement_mrid):
        sql = "INSERT INTO measurement(measurement_mrid) VALUES('{}')".format(measurement_mrid)
        self._create_connection()
        cur = self._conn.cursor()
        cur.execute(sql)
        cur.close()
        self._conn.commit()

    def create_measurement(self, measurement_mrid):
        if measurement_mrid not in self._measurements:
            self._insert_measurement(measurement_mrid)
            self._measurements[measurement_mrid] = {}

    def add_to_batch(self, measurement_mrid, sensor_prop, ts, original_value, sensor_value):
        self.create_measurement(measurement_mrid)
        self._batch.append([measurement_mrid, sensor_prop, ts, original_value, sensor_value])

    def _insert_batch(self, batch, conn):
        sql = """INSERT INTO record(measurement_mrid, property, ts, original, sensed)
                                    VALUES(?, ?, ?, ?, ?);"""
        cur = None
        try:
            cur = conn.cursor()
            cur.executemany(sql, batch)
            cur.close()
            conn.commit()
        except sqlite3.Error as e:
            _log.exception(e)
        batch.clear()

    def submit_batch(self):
        self._queue.put(self._batch.copy())
        self._batch.clear()
