from dbhandling import prozorro_db_queries
#from load_prozorro import *
from tender_telegram_bot import *
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date

def main():
    load_dotenv()
    prozorro_update, error_code = prozorro_db_queries.select_interesting_prozorro(os.getenv("PROZORRO_DB_NAME"),
                                                            start_date=date(2022, 6, 17),
                                                            end_date=date(2022, 6, 17))
    show_prozorro_data.show_interesting_prozorro(prozorro_update)

if __name__ == '__main__':
    main()
