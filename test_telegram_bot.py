import os
from dotenv import load_dotenv
from dbhandling import *
from load_prozorro import *
from tender_telegram_bot import *

class Telegram_Bot_Tests:
    """
    Tests for telegram bot API interaction
    """

    def __init__(self, str_data="", html_data="", json_data="", json_data_type=0):
        # uploading enviromental variables from ".env" file
        load_dotenv()
        TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        TELEGRAM_CHANNEL_CHAT_ID = os.getenv("TELEGRAM_CHANNEL1_CHAT_ID")
        TELEGRAM_CHANNEL_NAME = os.getenv("TELEGRAM_CHANNEL1_NAME")
        if (TELEGRAM_BOT_TOKEN == None) or (TELEGRAM_CHANNEL_CHAT_ID == None):
            sys.exit("Can't load data from .env")
        self.telegram_bot_token = TELEGRAM_BOT_TOKEN
        self.telegram_channel_chat_id = TELEGRAM_CHANNEL_CHAT_ID
        self.telegram_channel_name = TELEGRAM_CHANNEL_NAME
        self.str_data = str_data
        self.html_data = html_data
        self.json_data = json_data
        self.json_data_type = json_data_type

    def test_channel_publishing_str(self, str_data=""):
        if self.str_data == "":
            str_to_publish = self.str_data
        else:
            str_to_publish = "*bold \*text* \n \
        _italic \*text_ \n \
        __underline__ \n \
        ~strikethrough~ \n \
        ||spoiler|| \n \
        *bold _italic bold ~italic bold strikethrough ||italic bold strikethrough spoiler||~ __underline italic bold___ bold* \n \
        [inline URL](http://www.example.com/) \n \
        [inline mention of a user](tg://user?id=123456789) \n \
        `inline fixed-width code` \n \
        ``` \n \
        pre-formatted fixed-width code block \n \
        ``` \n \
        ```python \n \
        pre-formatted fixed-width code block written in the Python programming language \n \
        ```"
        send_result = telegram_channel_scripting.send_to_telegram(telegram_bot_token=self.telegram_bot_token,
                                                                  telegram_channel_chat_id=self.telegram_channel_chat_id,
                                                                  telegram_channel_name=self.telegram_channel_name,
                                                                  str_data=str_to_publish)
        print(send_result)
        return send_result

    def test_techchannel_publishing_str(self, err_module="Not a technical module", err_message=""):
        if err_message=="" and err_module=="Not a technical module":
            err_message = self.str_data
            if err_message=="":
                err_module = "ProZorro Test"
                err_message = "Something stopped working \|/...!@#$%^&*()|<>?"
        send_result = telegram_channel_scripting.raise_tech_message(telegram_bot_token=self.telegram_bot_token,
                                                                  err_module=err_module, err_message=err_message)
        print(send_result)
        return send_result

def main():
    tbt=Telegram_Bot_Tests(str_data="Test sent message")
#    tbt.test_channel_publishing_str()
    tbt.test_techchannel_publishing_str(err_module="Virshyky", err_message="50 pages")

if __name__ == '__main__':
    main()