import os.path
import json
import requests
from dotenv import load_dotenv
from dbhandling import *
from load_prozorro import search_api_prozorro


ENDPOINT_API = "https://public.api.openprocurement.org/api/2.5/tenders"
ENDPOINT_Site = "https://prozorro.gov.ua/search/tender"

def retrieve_single_tender_data_to_json(tender_id, api_key="", endpoint=ENDPOINT_API):
    """
    Retrieves info on single tender
    :param api_key: no API to read data from PROZORRO needed
    :param endpoint: connection URL to API
    :param tender_id:
    :return: response_json: JSON data on tender
        err_code:
         0 - no errors
        >0 - error code

    """
    response_json=""
    err_code=0
    params = {}
    tender_url = endpoint +"/" + tender_id

    try:
        requests.packages.urllib3.disable_warnings()
        response = requests.get(tender_url, verify=False, params=params)
        response.raise_for_status()
        response_json = response.json()

        load_dotenv()
        db_name=os.path.join(".",os.getenv("PROZORRO_DB_FOLDER"),"new_tender"+tender_id+".json")
        # print(db_name)
        with open(db_name, 'w', encoding='utf-8') as f:
            json.dump(response_json, f, ensure_ascii=False, indent=4)

    except requests.exceptions.RequestException as e:
        print("Connection error - can't read tenders info {}: {}".format(response.status_code, str(e)))
        err_code = 101

    return response_json, err_code

def retrieve_data(api_key="", endpoint=ENDPOINT_API, return_records_limit=100, offset=0.0, descending=0):
    """
    Retrieves tender headers list data
    :param api_key: no API to read data from PROZORRO needed
    :param endpoint: connection URL to API
    :param return_records_limit: limit of records per page
    :param offset: offset ID (of the last data uploaded)
    :param ascending:
        1 - from newest to oldest (needed for initial data upload)
        0 - from oldest to newest
    :return: err_code:
        0 - no errors
        >0 - error code
            next_offset: offset at the end of page
            next_url: url_of the next page

    """

    params = {}

    if return_records_limit>0:
        params["limit"]=return_records_limit

    if descending == 1:
        params["descending"] = descending

    if offset != 0.0:
        params["offset"] = offset
    else:
        offset = search_api_prozorro.find_first_row_offset()
        params["offset"] = offset

    err_code=0
    next_offset=""
    next_url=""

    try:
        requests.packages.urllib3.disable_warnings()
        response = requests.get(endpoint, verify=False, params=params)
        response.raise_for_status()
        response_json = response.json()
        print(response_json)

        load_dotenv()
        db_name = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), os.getenv("PROZORRO_DB_NAME"))
        # db_name=os.path.join(".",os.getenv("PROZORRO_DB_FOLDER"),"new_db_"+str(offset)+".json")
        # print(db_name)
        # with open(db_name, 'w', encoding='utf-8') as f:
        #     json.dump(response_json, f, ensure_ascii=False, indent=4)
        #     if not f.closed:
        #         f.close()
        prozorro_db_insertion.insert_update(db_filename=os.getenv("PROZORRO_DB_NAME"),
                                            tender_list_json_data=response_json,
                                            last_offset=offset,
                                            is_tender_list=True)
        # exit
        for tender_block in response_json["data"]:
            json_tender, err_code = retrieve_single_tender_data(tender_id=tender_block["id"])
        #     tender_date=datetime.fromisoformat(tender_block["dateModified"])
        # print(tender_date.date())
        # print(tender_date.time())

        next_offset = response_json["next_page"]["offset"]
        next_url = response_json["next_page"]["uri"]

    except requests.exceptions.RequestException as e:
        print("Connection error - code {}: {}".format(response.status_code, str(e)))
        err_code = 1

    return err_code, next_offset, next_url


