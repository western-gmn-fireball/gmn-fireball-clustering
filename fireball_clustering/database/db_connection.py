import sqlite3
import multiprocessing

class Database():
    lock = multiprocessing.Lock()

    def __init__(self) -> None:
        self.conn = sqlite3.connect('gmn_fireball_clustering.db')
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.cur = self.conn.cursor()
