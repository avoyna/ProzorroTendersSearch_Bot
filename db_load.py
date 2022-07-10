import datetime
import os
from dotenv import load_dotenv
from load_prozorro import *
from dbhandling import *
from datetime import datetime, timedelta, date

def create_prozorro_databases():
    load_dotenv()
    error_code = prozorro_db_create.create_db(os.getenv("PROZORRO_DB_NAME"))
    if error_code==0:
        error_code = prozorro_db_create.create_prozorro_tables(PROZORRO_DB_NAME)
        #prozorro_db_insertion.insert_update(PROZORRO_DB_NAME)
        pass

    return

def insert_prozorro_data():
    load_dotenv()
    # db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), os.getenv("PROZORRO_DB_NAME"))
    #
    #
    # err_code, offset, next_url = get_data_api_prozorro.retrieve_data(return_records_limit=10, offset=0.0,

    # ### simple db initializing with data erasing
    # prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"), is_initializing=True)

    ### load of 1 page of tenders (10 last tenders) without saving tender list and tender details
    ### accending since the last entry in Prozorro DB (no effect)
    # prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
    #                                   write_tender_list=False,
    #                                   write_tender_info=False)

    ###load of 1 page of tenders (10 last tenders) with saving tender list without tender details
    ### accending since the last entry in Prozorro DB (no effect)
    # prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
    #                                     write_tender_list=True,
    #                                     write_tender_info=False)

    ###load of 1 page of tenders (10 last tenders) with saving tender list without tender details
    # prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
    #                                     write_tender_list=True,
    #                                     write_tender_info=False,
    #                                     descending=1)

    ###load of 1 page of tenders (2 last tenders) with saving tender list and tender details
    # prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
    #                                     write_tender_list=True,
    #                                     write_tender_info=True,
    #                                     descending=1,
    #                                     return_records_limit=2)

    ###load all tenders of the specific date

    # load_date = date(2022, 7, 5)
    # for i in range(2):
        # load_date += timedelta(days=1)
        # prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
        #                                 write_tender_list=True,
        #                                 write_tender_info=True,
        #                                 single_date_to_load=load_date)

    ### load all data from the last successive download
    prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
                                        new_data_only=True, write_active_tenders_only=True)


def main():
    # err_code, offset, next_url = get_data_api_prozorro.retrieve_data(return_records_limit=100, descending=1)
    # print(err_code)
    # print(offset, next_url)
    # for i in range(100):
    #     err_code, offset, next_url = get_data_api_prozorro.retrieve_data(return_records_limit=100, offset=offset,
    #                                                                  descending=1)
    # print(err_code)
    # print(offset, next_url)
    DateS = date(2022, 7, 1)
    #print(DateS)
    first_offset_date = search_api_prozorro.find_begin_date_offset(DateS)
    print(first_offset_date)

    last_offset_date = search_api_prozorro.find_end_date_offset(DateS)
    print(last_offset_date)








if __name__ == '__main__':
    #main()
    #create_prozorro_databases()
    insert_prozorro_data()