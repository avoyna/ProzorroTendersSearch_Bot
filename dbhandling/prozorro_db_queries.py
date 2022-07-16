import os
import sqlite3
from dotenv import load_dotenv
from dbhandling import user_load_info_json, is_process_running
from tender_telegram_bot import telegram_channel_scripting

def find_last_successive_offset_in_db(db_filename):
    connection_obj = None
    load_dotenv()
    db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), db_filename)
    error_code = 0
    error_text = "Successful operation"

    try:
        connection_obj = sqlite3.connect(db_fn)
        cursor_obj = connection_obj.cursor()

        statement = """SELECT last_offset 
                    FROM Insertion_log 
                    WHERE error_code = 0 
                    ORDER BY insertion_ID DESC LIMIT 1;"""
        cursor_obj.execute(statement)

        fetch_result = cursor_obj.fetchone()
        if len(fetch_result)==0:
            last_offset=0.0
        else:
            last_offset=fetch_result[0]

    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                             err_module="Prozorro Local DB",
                                             err_message="Database last  " + db_fn + "." +
                                                         str(e.args[0]))
        error_code = -30

    finally:
        if connection_obj:
            connection_obj.close()

    if error_code==0:
        result=last_offset
    else:
        result=error_code

    return result


###""" select t.internal_ID, t.Tender_list_internal_ID, t.title, t.value_amount, t.value_currency, t.date, t.status, 
# t.tenderPeriod_endDate, t.enquiryPeriod_endDate, pe.name, pe.identifier_id, pe.address_region,
# pe.address_locality, i.description, i.classification_description, i.classification_id,
# i.deliveryAddress_region, i.deliveryAddress_locality, i.deliveryAddress_streetAddress
# from Tender t
# left join items i on i.tender_internal_ID = t.internal_ID
# left join procuringEntity pe on pe.internal_ID = t.procuringEntity_internal_ID
# where status like 'active%' and deliveryAddress_region like '%Київ%' 
# and (i.classification_id like '44%' or i.classification_id like '45%' or i.classification_id like '505%' 
# or i.classification_id like '507%' or i.classification_id like '508%' or i.classification_id like '511%' 
# or i.classification_id like '512%' or i.classification_id like '515%' or i.classification_id like '517%' 
# or i.classification_id like '518%' or i.classification_id like '519%' or i.classification_id like '71%')
# and value_amount >= 20000
# and t.dateModified >= '2022-06-05'
# group by t.internal_ID
# order by Tender_list_internal_ID desc;"""

def select_interesting_prozorro(db_filename, last_viewed_tender_time=None, start_date=None, end_date=None,
                                update_from_last=False, username="user1"):
    resulting_info = [[]]
    connection_obj = None
    load_dotenv()
    db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), db_filename)
    error_code = 0
    upload_runs = is_process_running.is_running("db_load.py")

    if update_from_last:
        json_user_data=user_load_info_json.get_user_last_db_load_info_from_json_file(username=username)
        if (len(json_user_data)>0) and (username in json_user_data):
            resulting_info, error_code = select_interesting_prozorro(db_filename=db_filename,
                                      last_viewed_tender_time=json_user_data[username]["dateModified"])
        else:
            resulting_info, error_code = select_interesting_prozorro(db_filename=db_filename,
                                                                     start_date=start_date)
    elif (end_date) or (not upload_runs):
        try:
            connection_obj = sqlite3.connect(db_fn)
            cursor_obj = connection_obj.cursor()

            statement = """SELECT t.internal_ID, tl.id, t.title, t.value_amount, 
                t.value_currency, t.dateModified, t.status, t.tenderPeriod_endDate, t.enquiryPeriod_endDate, 
                pe.name, pe.identifier_id, pe.address_region, pe.address_locality, i.description, 
                i.classification_description, i.classification_id, i.deliveryAddress_region, 
                i.deliveryAddress_locality, i.deliveryAddress_streetAddress, t.tenderID
                    FROM Tender t
                    LEFT JOIN items i ON i.tender_internal_ID = t.internal_ID
                    LEFT JOIN procuringEntity pe ON pe.internal_ID = t.procuringEntity_internal_ID
                    LEFT JOIN Tender_list tl ON tl.internal_ID = t.Tender_list_internal_ID
                    WHERE status LIKE 'active%' and deliveryAddress_region LIKE '%Київ%' 
                    AND ( (i.classification_id LIKE '45%' AND i.classification_id NOT LIKE '4545%')
                    OR (i.classification_id LIKE '4545%' and i.description NOT LIKE '%асфальт%')
                    OR (i.classification_id LIKE '507%' AND i.classification_id NOT LIKE '5073%')  
                    OR i.classification_id LIKE '508%' OR i.classification_id LIKE '511%' 
                    OR i.classification_id LIKE '512%' OR i.classification_id LIKE '515%' OR i.classification_id LIKE '517%' 
                    OR i.classification_id LIKE '518%' OR i.classification_id LIKE '519%' 
                    OR (i.classification_id LIKE '71%' AND i.classification_id NOT LIKE '7152%' 
                     AND i.classification_id NOT LIKE '7134%' AND i.classification_id NOT LIKE '719%')
                    OR (i.classification_id LIKE '7152%' and i.description NOT LIKE '%асфальт%')
                    OR (i.classification_id LIKE '7152%' and i.description NOT LIKE '%рад_ац%')
                    OR (i.classification_id LIKE '7134%' and i.description NOT LIKE '%землеустр%')
                    OR (i.classification_id LIKE '7134%' and i.description NOT LIKE '%норматив%грош%оц_нк%')
                    OR i.classification_id LIKE '99%')
                    AND i.classification_id NOT LIKE '4523%' AND i.classification_id NOT LIKE '4524%'
                    AND i.classification_id NOT LIKE '4525%'
                    AND t.value_amount >= 20000 """
                    # excluded: i.classification_id LIKE '44%' OR i.classification_id LIKE '505%'
                    # AND t.dateModified >= '2022-07-05'


            if start_date:
                statement += """
                AND DATE(t.dateModified) >= ?"""
                if end_date:
                    statement += """
                AND DATE(t.dateModified) <= ?"""
                    value = (start_date, end_date)
                else:
                    value = (start_date,)
                statement += """
                    ORDER BY t.Tender_list_internal_ID ASC;"""
                cursor_obj.execute(statement, value)

            elif last_viewed_tender_time:
                statement += """
                AND t.dateModified > ?"""
                if end_date:
                    statement += """
                AND DATE(t.dateModified) <= ?"""
                    value = (last_viewed_tender_time, end_date)
                else:
                    value = (last_viewed_tender_time, )

                statement += """
                    ORDER BY t.Tender_list_internal_ID ASC;"""
                cursor_obj.execute(statement, value)

            elif end_date:
                statement += """
                            AND DATE(t.dateModified) <= ?"""
                value = (end_date,)
                statement += """
                            ORDER BY t.Tender_list_internal_ID ASC;"""
                cursor_obj.execute(statement, value)


            else:
                statement += """
                    ORDER BY t.Tender_list_internal_ID ASC;"""
                cursor_obj.execute(statement)

            resulting_info = cursor_obj.fetchall()

            if not end_date:
                error_code = user_load_info_json.save_user_last_db_load_info_to_json_file(db_connection=connection_obj,
                                                                             db_cursor=cursor_obj,
                                                                             username=username)
        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                          err_module="Prozorro Local DB",
                                                          err_message="Database last  " + db_fn + "." +
                                                                      str(e.args[0]))
            error_code = -30

        finally:
            if connection_obj:
                connection_obj.close()

        #resulting_info.append(resulting_info[len(resulting_info)-1])

    return resulting_info, error_code