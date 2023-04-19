import os
from datetime import datetime
from time import perf_counter
from dbengine import DBEngine
from functools import wraps
import pandas as pd

global SESSION_TIMING = []
class InitQuery:
    def __init__(self, csv_file):
        self.in_csv = csv_file
        self.engine = DBEngine(self.in_csv)
        self.file_size = self.get_csv_size()

    def get_csv_size(self):
        return os.path.getsize(self.in_csv)
    def __del__(self):
        SESSION_TIMING = []
        del self.engine
    @print_timing
    def run(self):
        self.engine.create_inmemory_connection()
        self.engine.create_table()
        self.init_query = self.engine.query_table_first_numeric_column()
        timing = [self.file_size]+SESSION_TIMING[-1]
        return f"Loaded and Analyzed {timing[0]} of data in {timing[2]} seconds on {timing[3]} with DuckDb"

    def persist_database(self):
        self.engine.create_persistent_connection_local()

def print_timing(func):
    '''
    create a timing decorator function
    use
    @print_timing
    just above the function you want to time
    '''
    @wraps(func)
    def wrapper(*arg):
        start = perf_counter()
            result = func(*arg)
            end = perf_counter()
            fs = '{} took {:.3f} microseconds'
            SESSION_TIMING.append((fs, (end - start)*1000000, datetime.now()))
            print(fs.format(func.__name__, (end - start)*1000000))
            return result
        return wrapper