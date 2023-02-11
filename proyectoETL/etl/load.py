
from utils import utils as u

class Load():
    def __init__(self) -> None:
        self.process = 'Load Process'

    def load_postgresql(self, df, db_tbl_name):
        
        u.df_to_postgresql(df, u.postgresql_engine(), db_tbl_name)