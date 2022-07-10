import datetime
import os
from dotenv import load_dotenv
from load_prozorro import *
from dbhandling import *
from datetime import datetime, timedelta, date

DELETE_DAY_RETROSPECTIVE = 60

def delete_old_data():
    load_dotenv()
    min_date_to_keep_in_base = (datetime.now()-timedelta(days=DELETE_DAY_RETROSPECTIVE)).date()
    prozorro_db_clear_data.delete_old_data(db_filename=os.getenv("PROZORRO_DB_NAME"),
                                           min_date_to_keep_in_base=min_date_to_keep_in_base)
    return


def main():
    pass

if __name__ == '__main__':
    delete_old_data()