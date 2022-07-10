from tender_telegram_bot import telegram_channel_scripting, MarkdownV2_conversions
from dotenv import load_dotenv
from load_prozorro import get_data_api_prozorro
from datetime import datetime
import os

def show_interesting_prozorro(prozorro_info):
    load_dotenv()
    items_text = ""
    items_text_count = 0

    for message_id, message in enumerate(prozorro_info):
        if (len(prozorro_info) > message_id+1) and (message[0]==prozorro_info[message_id+1][0]):
            if items_text_count < 4:
                if not message[13] == None:
                    items_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[13]) + "`\n"
                # if not message[15] == None:
                #     items_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[15]) + \
                #                     ", " + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[14]) + "`\n"
                # if not message[16] == None:
                #     items_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[16]) + "`"
                # if not message[17] == None:
                #     items_text += ", `" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[17]) + "`"
                # if not message[18] == None:
                #     items_text += ", `" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[18]) + "`"
                items_text += "\n"
            items_text_count += 1

        elif prozorro_info!=[[]]:
            message_text = ""

            ### Uploaded fields in order
            # 0 t.internal_ID, 1 tl.id, 2 t.title, 3 t.value_amount,
            # 4 t.value_currency, 5 t.dateModified, 6 t.status, 7 t.tenderPeriod_endDate, 8 t.enquiryPeriod_endDate,
            # 9 pe.name, 10 pe.identifier_id, 11 pe.address_region, 12 pe.address_locality, 13 i.description,
            # 14 i.classification_description, 15 i.classification_id, 16 i.deliveryAddress_region,
            # 17 i.deliveryAddress_locality, 18 i.deliveryAddress_streetAddress, 19 t.tenderID

            if not message[9] == None:
                message_text += "*" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[9]) + ", *"
            if not message[12] == None:
                message_text += "*_" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[12]) + "_*"
            if not message[2] == None:
                message_text += "\n[*" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[2]) + \
                                "*](" + get_data_api_prozorro.ENDPOINT_Site + "?text=" +\
                                MarkdownV2_conversions.add_telegram_text_escaped_characters(message[1]) + ")\n"
            if not message[5] == None:
                message_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(
                    datetime.fromisoformat(message[5]).strftime("%d.%m.%Y")) + "`\n"
            if not message[3] == None:
                message_text += "*" + MarkdownV2_conversions.add_telegram_text_escaped_characters(
                    "{:,.2f}".format(message[3]).replace(',', ' ')) + " " + \
                                MarkdownV2_conversions.add_telegram_text_escaped_characters(message[4]) + "* \n"
            if not message[6] == None:
                match message[6]:
                    case "active":
                        status = "Активний тендер"
                    case "active.enquiries":
                        status = "Період уточнень"
                    case "active.tendering":
                        status = "Період аукціону"
                    case "active.qualification":
                        status = "Кваліфікація переможця"
                    case "active.awarded":
                        status = "Пропозиції розглянуто"
                    case "unsuccessful":
                        status = "Закупівля не відбулась"
                    case "complete":
                        status = "Закупівля завершена"
                    case "cancelled":
                        status = "Відмінена закупівля"
                    case "active.pre-qualification":
                        status = "Перед-кваліфікаційний період"
                    case "active.pre-qualification.stand-still":
                        status = "Блокування перед аукціоном"
                    case _:
                        status = message[6]

                message_text += "_" + MarkdownV2_conversions.add_telegram_text_escaped_characters(status) + "_\n"

            if not message[19] == None:
                message_text += "_ID: " + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[19]) + "_\n"

            message_text += items_text
            items_text = ""
            items_text_count = 0
            if not message[13] == None:
                message_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[13]) + "`\n"
            if not message[15] == None:
                message_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[15]) + \
                                ", " + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[14]) + "`\n"
            if not message[16] == None:
                message_text += "`" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[16]) + "`"
            if not message[17] == None:
                message_text += ", `" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[17]) + "`"
            if not message[18] == None:
                message_text += ", `" + MarkdownV2_conversions.add_telegram_text_escaped_characters(message[18]) + "`"

            if not message[7] == None:
                message_text += "\n`Тендер до: " + MarkdownV2_conversions.add_telegram_text_escaped_characters(
                    datetime.fromisoformat(message[7]).strftime("%d.%m.%Y"))  + "`\n"
            if not message[8] == None:
                message_text += "`Заявки до: " +MarkdownV2_conversions.add_telegram_text_escaped_characters(
                    datetime.fromisoformat(message[8]).strftime("%d.%m.%Y"))  + "`\n"


            telegram_channel_scripting.send_to_telegram(telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN"),
                             telegram_channel_chat_id = os.getenv("TELEGRAM_CHANNEL1_CHAT_ID"),
                             telegram_channel_name = os.getenv("TELEGRAM_CHANNEL1_NAME"),
                             str_data=message_text)

    return
