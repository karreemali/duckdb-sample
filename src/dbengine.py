import os
import pandas as pd
from dataclasses import dataclass
import duckdb
from uuid import uuid4

@dataclass
class DBEngine:
    file_path: str
    def __init__(self):
        self.conn = self.create_inmemory_connection()
        self.uuid = "".join(str(uuid4()).split("-"))
        self.query = pd.DataFrame()
    def __del__(self):
        try:
            del self.conn
        except Exception as e:
            print(f"Unable to remove connection: {self.file_path}")
        if os.path.exists(self.file_path):
            try:
                os.remove(self.file_path)
            except IOError:
                print(f"Unable to remove file: {self.file_path}")

    def create_inmemory_connection(self):
        return duckdb.connect(database=':memory:')

    def create_persistent_connection_local(self):
        '''
        create a persistent local duckdb connection using name of file.
        :return:
        '''
        return duckdb.connect(database=f"{os.path.splitext(self.file_path)[0]}.db")

    def create_table(self):
        self.data = self.conn.execute(f"CREATE TABLE '{self.uuid}' AS SELECT * FROM '{self.file_path}';")
        return self.data

    def table_headers_str(self) -> list:
        return self.data.columns

    def first_num_header(self):
        num_header = None
        for col in self.query.description:
            if col[0].upper() == 'NUMBER':
                num_header = col[0]
            break
        return num_header

    def get_first_column(self):
        return self.query.description[0][0]

    def query_table_first_numeric_column(self) -> pd.DataFrame:
        try:
            num_header = self.first_num_header()
            if num_header:
                self.query = self.conn.execute(
                    f"SELECT {','.join(self.table_headers_str())} FROM {self.uuid} where {num_header} >  5000 order by {self.get_first_column()}").df()
                )
            else:
                raise AssertionError(f"No numerical column found for {os.path.basename(self.file_path)}")
            return self.query
        except Exception as e:
            print(f"Unable to generate query: {e}")
