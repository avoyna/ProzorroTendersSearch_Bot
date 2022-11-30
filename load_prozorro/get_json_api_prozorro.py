import requests
import os
from dotenv import load_dotenv
from tender_telegram_bot import telegram_channel_scripting
from load_prozorro import get_data_api_prozorro, search_api_prozorro

def retrieve_json_tender_list(api_key="", endpoint="", return_records_limit=100, offset=0.0, descending=0):
    """
    Retrieves tender headers list data
    :param api_key: no API to read data from PROZORRO needed
    :param endpoint: connection URL to API
    :param return_records_limit: limit of records per page
    :param offset: offset ID (of the last data uploaded)
    :param descending:
        1 - from newest to oldest (needed for initial data upload)
        0 - from oldest to newest
    :return: response_json: JSON with tenders list
        err_code:
            0 - no errors
            <0 - error code
                next_offset: offset at the end of page
                next_url: url_of the next page

    """
    load_dotenv()
    if endpoint=="":
        endpoint = get_data_api_prozorro.ENDPOINT_API

    response=None
    response_json= {}
    err_code = 0
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

    params["verify"] = False

    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        response_json = response.json()

    except requests.exceptions.RequestException as e:
        if response == None:
            print("Connection error - code {}: {}".format("1000", str(e)))
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                err_module="Prozorro Local DB",
                err_message="Load from Prozorro Tender_list data error. No connection." +
                            str(e.args[0]))
            err_code = -1011
        else:
            print("Connection error - code {}: {}".format(response.status_code, str(e)))
            telegram_channel_scripting.raise_tech_message(
                telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                err_module="Prozorro Local DB",
                err_message="Load from Prozorro Tender_list data error. " +
                            str(e.args[0]))
            err_code = -11

    return response_json, err_code


def retrieve_single_tender_data_to_json(tender_id, api_key="", endpoint=""):
    """
    Retrieves info on single tender
    :param api_key: no API to read data from PROZORRO needed
    :param endpoint: connection URL to API
    :param tender_id:
    :return: response_json: JSON data on tender
        err_code:
         0 - no errors
        <0 - error code

    """
    if endpoint=="":
        endpoint = get_data_api_prozorro.ENDPOINT_API

    response_json=""
    err_code=0
    params = {}
    tender_url = endpoint +"/" + tender_id

    try:
        response = requests.get(tender_url, params=params)
        response.raise_for_status()
        response_json = response.json()

    except requests.exceptions.RequestException as e:
        print("Connection error - can't read tenders info: {}".format(str(e)))
        telegram_channel_scripting.raise_tech_message(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            err_module="Prozorro Local DB",
            err_message="Load from Prozorro Tender data error. " + str(e.args[0]))
        err_code = -101

    return response_json, err_code
