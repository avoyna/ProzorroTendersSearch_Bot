import os
from dotenv import load_dotenv
from load_prozorro import *
from datetime import *


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
    main()