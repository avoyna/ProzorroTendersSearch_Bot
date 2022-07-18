import requests
import os
import time
from tender_telegram_bot import MarkdownV2_conversions


def send_to_telegram(telegram_bot_token, telegram_channel_chat_id, telegram_channel_name, str_data="", html_data="",
                     json_data="", json_data_type=0):

    """ Sends messages to Telegram in different formats
    :param telegram_bot_token: TELEGRAM_BOT_TOKEN
    :param telegram_channel_chat_id: TELEGRAM_CHANNEL_CHAT_ID
    :param telegram_channel_name: TELEGRAM_CHANNEL_NAME
    :param str_data: passed data in raw string
    :param html_data: passed data in formatted HTML
    :param json_data: passed data in JSON
    :param json_data_type: if data in JSON
        1 - Prozorro Tenders data
        0 - other data
    :return: code if sending is successful
        0 - success
        >0 - fail
        -1 - no data
    """

    url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
    if str_data != "":
        params = {"chat_id": telegram_channel_chat_id, "text": str_data, "parse_mode": "MarkdownV2"}
        try:
            response = requests.get(url, params=params)

            response.raise_for_status()
            time.sleep(5)

            response_json = response.json()
            # print(response_json)
        except requests.exceptions.RequestException as e:
            print("Connection error - code {}: {}".format(response.status_code, str(e)))
            raise_tech_message(telegram_bot_token=telegram_bot_token,
                               err_module="ProZorro Bot - "+telegram_channel_name,
                               err_message="String message can't be delivered. \n" + str(e))
            return response.status_code

    elif html_data != "":
        params = {"chat_id": telegram_channel_chat_id, "text": html_data, "parse_mode": "HTML"}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            response_json = response.json()
        except requests.exceptions.RequestException as e:
            print("Connection error - code {}: {}".format(response.status_code, str(e)))
            raise_tech_message(telegram_bot_token=telegram_bot_token,
                               err_module="ProZorro Bot - " + telegram_channel_name,
                               err_message="HTML message can't be delivered. \n" + e)
            return response.status_code

    elif json_data != "":
        params = {"chat_id": telegram_channel_chat_id, "text": json_data, "parse_mode": "MarkdownV2"}
        if json_data_type == 1:
            print("JSON not supported. As well as ProZorro")
        else:
            print("JSON not supported")
        return 1
    else:
        return -1

    # print(response)
    return 0

def raise_tech_message(telegram_bot_token, err_module, err_message):
    """
    Send messages to technical Telegram channel (defined in .env file)
    :param telegram_bot_token: TELEGRAM_BOT_TOKEN
    :param err_module: Error module sending a message
    :param err_message: Error message
    :return: code if sending is successful
        0 - success
        >0 - fail
        -1 - no data
    """
    TELEGRAM_TECH_CHANNEL_CHAT_ID = os.getenv("TELEGRAM_TECH_CHANNEL_CHAT_ID")
    TELEGRAM_TECH_CHANNEL_NAME = os.getenv("TELEGRAM_TECH_CHANNEL_NAME")
    if (TELEGRAM_TECH_CHANNEL_CHAT_ID == None):
        sys.exit("No TelegramTechChannel ID")

    result_str=send_to_telegram(telegram_bot_token=telegram_bot_token,
                                telegram_channel_chat_id=TELEGRAM_TECH_CHANNEL_CHAT_ID,
                                telegram_channel_name=TELEGRAM_TECH_CHANNEL_NAME,
                                str_data=MarkdownV2_conversions.format_tech_message_markdownV2(err_module, err_message))

    return result_str



