import requests
from datetime import datetime
from load_prozorro import get_data_api_prozorro #ENDPOINT_API, ENDPOINT_Site


def find_begin_date_offset(search_date):
    """
    Searches fo the offset shortcut to the 1st entry on the day within the DB
    :param search_date: data to find offset shortcut to the 1st entry on the day within the DB
    :return:
        positive number - offset shortcut
        negative number - error code
            -1 - general error
            -2 - date to find is grater than last date within the DB
    """

    offset_id = -1 # error code
    output_limit = 1000

    params = {}
    params["limit"] = output_limit
    params["descending"] = 1
    try:
        response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
        response.raise_for_status()
        response_json = response.json()

    except requests.exceptions.RequestException as e:
        print("Connection error - code {}: {}".format(response.status_code, str(e)))

    if not response_json["data"] == []:
        last_isodate_on_load = response_json["data"][0]["dateModified"]
        if search_date > datetime.fromisoformat(last_isodate_on_load).date():
            offset_id = -2 # error code if date to find is grater than last date within the DB
        else:
            prev_isodate_on_load = last_isodate_on_load
            last_isodate_on_load = response_json["data"][-1]["dateModified"]
            next_offset = response_json["next_page"]["offset"]
            offset_id = next_offset
            # print(datetime.fromisoformat(last_isodate_on_load).date())
            # exit()
            while search_date <= datetime.fromisoformat(last_isodate_on_load).date() and \
                    (not (response_json["data"] == [])):
                params = {}
                if (not (next_offset is None)):
                    params["offset"] = next_offset
                params["limit"] = output_limit
                params["descending"] = 1

                try:
                    response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
                    # print(response.request.url)
                    response.raise_for_status()
                    response_json = response.json()
                    prev_isodate_on_load = last_isodate_on_load
                    last_isodate_on_load = response_json["data"][-1]["dateModified"]
                    offset_id = next_offset
                    next_offset = response_json["next_page"]["offset"]

                except requests.exceptions.RequestException as e:
                    print("Connection error - code {}: {}".format(response.status_code, str(e)))

            while output_limit>1:
                next_offset = offset_id
                last_isodate_on_load = prev_isodate_on_load
                output_limit = int(output_limit / 10)

                while search_date<=datetime.fromisoformat(last_isodate_on_load).date() and \
                        (not (response_json["data"] == "")):
                    params = {}
                    if (not (next_offset is None)):
                        params["offset"] = next_offset
                    params["limit"] = output_limit
                    params["descending"] = 1

                    try:
                        response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
                        # print(response.request.url)
                        response.raise_for_status()
                        response_json = response.json()
                        prev_isodate_on_load = last_isodate_on_load
                        last_isodate_on_load = response_json["data"][-1]["dateModified"]
                        offset_id = next_offset
                        next_offset = response_json["next_page"]["offset"]

                    except requests.exceptions.RequestException as e:
                        print("Connection error - code {}: {}".format(response.status_code, str(e)))


            # params = {}
            # if (not (next_offset is None)):
            #     params["offset"] = next_offset
            # params["limit"] = output_limit
            # params["descending"] = 1
            #
            # try:
            #     response = requests.get(ENDPOINT_API, params=params)
            #     response.raise_for_status()
            #     response_json = response.json()
            #     offset_id = response_json["next_page"]["offset"]

            # except requests.exceptions.RequestException as e:
            #     print("Connection error - code {}: {}".format(response.status_code, str(e)))


    # print(response.request.url)
    offset_id=next_offset
    # print(offset_id)

    return offset_id


def find_end_date_offset(search_date):
    """
    Searches fo the offset shortcut to the last entry on the day within the DB
    :param search_date: data to find offset shortcut to the last entry on the day within the DB
    :return:
        positive number - offset shortcut
        negative number - error code
            -1 - general error
    """

    offset_id = -1
    output_limit = 1000

    params = {}
    params["limit"] = output_limit
    params["descending"] = 1
    try:
        response = requests.get(ENDPOINT_API, params=params)
        response.raise_for_status()
        response_json = response.json()

    except requests.exceptions.RequestException as e:
        print("Connection error - code {}: {}".format(response.status_code, str(e)))

    if not response_json["data"] == []:
        last_isodate_on_load = response_json["data"][-1]["dateModified"]
        prev_isodate_on_load = last_isodate_on_load
        next_offset = response_json["next_page"]["offset"]
        # print(datetime.fromisoformat(last_isodate_on_load).date())
        # exit()
        while search_date < datetime.fromisoformat(last_isodate_on_load).date() and \
                (not (response_json["data"] == [])):
            params = {}
            if (not (next_offset is None)):
                params["offset"] = next_offset
            params["limit"] = output_limit
            params["descending"] = 1

            try:
                response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
                # print(response.request.url)
                response.raise_for_status()
                response_json = response.json()
                prev_isodate_on_load = last_isodate_on_load
                last_isodate_on_load = response_json["data"][-1]["dateModified"]
                offset_id = next_offset
                next_offset = response_json["next_page"]["offset"]

            except requests.exceptions.RequestException as e:
                print("Connection error - code {}: {}".format(response.status_code, str(e)))


        while output_limit>1:
            next_offset=offset_id
            last_isodate_on_load  = prev_isodate_on_load
            output_limit = int(output_limit/10)

            while search_date < datetime.fromisoformat(last_isodate_on_load).date() and \
                    (not (response_json["data"] == "")):

                params = {}
                if (not (next_offset is None)):
                    params["offset"] = next_offset
                params["limit"] = output_limit
                params["descending"] = 1

                try:
                    response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
                    response.raise_for_status()
                    response_json = response.json()
                    prev_isodate_on_load = last_isodate_on_load
                    last_isodate_on_load = response_json["data"][-1]["dateModified"]
                    offset_id = next_offset
                    next_offset = response_json["next_page"]["offset"]

                except requests.exceptions.RequestException as e:
                    print("Connection error - code {}: {}".format(response.status_code, str(e)))


        params = {}
        if (not (next_offset is None)):
            params["offset"] = next_offset
        params["limit"] = output_limit
        params["descending"] = 1

        try:
            response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
            response.raise_for_status()
            response_json = response.json()
            offset_id = response_json["next_page"]["offset"]

        except requests.exceptions.RequestException as e:
            print("Connection error - code {}: {}".format(response.status_code, str(e)))

    # print(response.request.url)
    # print(offset_id)

    return offset_id

def find_first_row_offset():
    offset_id = -1

    params = {}
    params["limit"] = 1
    params["descending"] = 1
    try:
        response = requests.get(get_data_api_prozorro.ENDPOINT_API, params=params)
        response.raise_for_status()
        response_json = response.json()
        offset_id = response_json["next_page"]["offset"]
    except requests.exceptions.RequestException as e:
        print("Connection error - code {}: {}".format(response.status_code, str(e)))


    return offset_id
