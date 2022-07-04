import os
from dotenv import load_dotenv
from dbhandling import *
from load_prozorro import *
from tender_telegram_bot import *

def main():
    # uploading enviromental variables from ".env" file
    load_dotenv()
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHANNEL_CHAT_ID = os.getenv("TELEGRAM_CHANNEL1_CHAT_ID")
    if (TELEGRAM_BOT_TOKEN == None) or (TELEGRAM_CHANNEL_CHAT_ID == None):
        sys.exit("Can't load data from .env")

    send_result = send_to_telegram()

if __name__ == '__main__':
    main()
