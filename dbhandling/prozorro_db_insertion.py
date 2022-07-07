import sqlite3
import os.path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from tender_telegram_bot import telegram_channel_scripting
from load_prozorro import get_json_api_prozorro, search_api_prozorro
from dbhandling import prozorro_db_create, prozorro_db_queries


def insert_update(db_filename, last_offset=-1, write_tender_list=False, write_tender_info=False, pages_limit=1,
                  return_records_limit=10, offset=0.0, descending=0, is_initializing=False,
                  single_date_to_load=None, new_data_only=False):

    connection_obj = None
    load_dotenv()
    db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), db_filename)
    error_code = 0
    error_text = "Successful operation"
    insertion_log_id = 0

    if is_initializing:
        prozorro_db_create.drop_data_tables(db_filename=db_filename)
        descending = 1
        offset = 0.0
        prozorro_db_create.create_prozorro_tables(db_filename=db_filename)

    else:
        if last_offset==-1:
            last_offset = search_api_prozorro.find_first_row_offset()

        try:
            connection_obj = sqlite3.connect(db_fn)
            cursor_obj = connection_obj.cursor()

            statement = """INSERT INTO Insertion_log (insertion_datetime, inserted_by_user, error_code, 
                        error_text, last_offset) VALUES (?, ?, ?, ?, ?); """

            currentDateTime = datetime.now()

            values = (currentDateTime, "db_creator", "-400", "Adding information to DB not finished", None)

            cursor_obj.execute(statement, values)
            connection_obj.commit()

            cursor_obj.execute("SELECT insertion_ID FROM Insertion_log ORDER BY insertion_ID DESC LIMIT 1")
            insertion_log_id = cursor_obj.fetchone()[0]

            offset_q = offset

            if new_data_only:
                write_tender_list = True
                write_tender_info = True
                offset_q = prozorro_db_queries.find_last_successive_offset_in_db(db_filename)
                tender_list_json_data, error_code = get_json_api_prozorro.retrieve_json_tender_list(
                    return_records_limit=return_records_limit,
                    offset=offset_q, descending=0)
                if write_tender_list and (error_code == 0):
                    error_code = insert_Tender_list(db_connection=connection_obj,
                                                    db_cursor=cursor_obj,
                                                    json_data=tender_list_json_data,
                                                    log_id=insertion_log_id,
                                                    write_tender_info=write_tender_info,
                                                    single_date_to_load=single_date_to_load)

                while (tender_list_json_data["data"] != []) and (error_code == 0):
                    offset_q = tender_list_json_data["next_page"]["offset"]
                    tender_list_json_data, error_code = get_json_api_prozorro.retrieve_json_tender_list(
                        return_records_limit=return_records_limit,
                        offset=offset_q, descending=0)
                    if write_tender_list and (error_code == 0):
                        error_code = insert_Tender_list(db_connection=connection_obj,
                                                        db_cursor=cursor_obj,
                                                        json_data=tender_list_json_data,
                                                        log_id=insertion_log_id,
                                                        write_tender_info=write_tender_info,
                                                        single_date_to_load=single_date_to_load)

                        if error_code != 0:
                            error_text = "Error with tender list upload"
                        connection_obj.commit()

                offset_q = tender_list_json_data["next_page"]["offset"]
                last_offset = offset_q

            elif pages_limit>1:
                for i in range(pages_limit):
                    tender_list_json_data, error_code = get_json_api_prozorro.retrieve_json_tender_list(
                        return_records_limit=return_records_limit,
                        offset=offset_q, descending=descending)

                    offset_q = tender_list_json_data["data"]["next_page"]["offset"]

                    if write_tender_list and (error_code==0):
                        Tender_list_internal_ID, error_code = insert_Tender_list(db_connection=connection_obj,
                                                        db_cursor=cursor_obj,
                                                        json_data=tender_list_json_data,
                                                        log_id=insertion_log_id,
                                                        write_tender_info=write_tender_info)
                        if error_code != 0:
                            error_text = "Error with tender list upload"
                        connection_obj.commit()

                last_offset = offset_q

            elif (single_date_to_load != None):
                offset_q = search_api_prozorro.find_begin_date_offset(single_date_to_load)
                tender_list_json_data, error_code = get_json_api_prozorro.retrieve_json_tender_list(
                    return_records_limit=return_records_limit,
                    offset=offset_q, descending=0)
                if write_tender_list and (error_code==0):
                    error_code = insert_Tender_list(db_connection=connection_obj,
                                                    db_cursor=cursor_obj,
                                                         json_data=tender_list_json_data,
                                                         log_id=insertion_log_id,
                                                         write_tender_info=write_tender_info,
                                                         single_date_to_load=single_date_to_load)

                while (tender_list_json_data["data"]!=[]) and (error_code==0) and \
        (datetime.fromisoformat(tender_list_json_data["data"][0]["dateModified"]).date()==single_date_to_load):
                    offset_q = tender_list_json_data["next_page"]["offset"]
                    tender_list_json_data, error_code = get_json_api_prozorro.retrieve_json_tender_list(
                        return_records_limit=return_records_limit,
                        offset=offset_q, descending=0)
                    if write_tender_list and (error_code==0):
                        error_code = insert_Tender_list(db_connection=connection_obj,
                                                        db_cursor=cursor_obj,
                                                             json_data=tender_list_json_data,
                                                             log_id=insertion_log_id,
                                                             write_tender_info=write_tender_info,
                                                             single_date_to_load=single_date_to_load)


                        if error_code != 0:
                            error_text = "Error with tender list upload"
                        connection_obj.commit()

                offset_q = search_api_prozorro.find_begin_date_offset(single_date_to_load + timedelta(days=1))
                if offset_q<0:
                    offset_q = search_api_prozorro.find_first_row_offset()
                last_offset = offset_q

            else:
                tender_list_json_data, error_code = get_json_api_prozorro.retrieve_json_tender_list(
                    return_records_limit=return_records_limit,
                    offset=offset_q, descending=descending)

                if write_tender_list and (error_code==0):
                    error_code = insert_Tender_list(db_connection=connection_obj,
                                                    db_cursor=cursor_obj,
                                                    json_data=tender_list_json_data,
                                                    log_id=insertion_log_id,
                                                    write_tender_info=write_tender_info)
                    if error_code != 0:
                        error_text = "Error with tender list upload"
                    connection_obj.commit()

                last_offset = offset_q


            statement = """ UPDATE 
                            Insertion_log
                        SET    
                            error_code = ?, error_text = ?, last_offset = ?
                        WHERE 
                            insertion_ID = ?;"""
            values = (error_code, error_text, last_offset, insertion_log_id)

            # print(values)
            cursor_obj.execute(statement, values)
            connection_obj.commit()

        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                 err_module="Prozorro Local DB",
                                                 err_message="Database data insertion error in " + db_fn + "." + e.args[0])
            error_code = -1

        finally:
            if connection_obj:
                connection_obj.close()

    return error_code


def insert_Tender_list(db_connection, db_cursor, json_data, log_id, write_tender_info=False, single_date_to_load=None):
    error_code = 0

    params = []
    if json_data["data"]!=[]:
        for tender_id in json_data["data"]:
            if (not single_date_to_load) or \
                    (datetime.fromisoformat(tender_id["dateModified"]).date() == single_date_to_load):
                params.append((tender_id["id"], tender_id["dateModified"], log_id))

        statement = "INSERT INTO Tender_List (id, dateModified, insertion_ID) VALUES (?, ?, ?); "

        try:
            db_cursor.executemany(statement, params)
            db_connection.commit()

            if write_tender_info and (json_data["data"]!=[]):
                for tender in json_data["data"]:

                    tender_json, error_code = get_json_api_prozorro.retrieve_single_tender_data_to_json(
                        tender_id=tender["id"])
                    if error_code==0:
                        if (not single_date_to_load) or \
                                (datetime.fromisoformat(tender["dateModified"]).date()==single_date_to_load):
                            statement = """SELECT internal_ID 
                                        FROM Tender_list 
                                        WHERE id = ?
                                        ORDER BY internal_ID DESC LIMIT 1;"""
                            value = (tender["id"],)
                            db_cursor.execute(statement, value)
                            insertion_tender_id = db_cursor.fetchone()[0]

                            Tender_internal_ID, error_code = insert_Tender(db_connection=db_connection,
                                                db_cursor=db_cursor,
                                                json_data=tender_json["data"],
                                                Tender_list_internal_ID=insertion_tender_id)
                            if error_code != 0:
                                error_text = "Error writing tenders"
                                exit
                    else:
                        exit


        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                          err_module="Prozorro Local DB",
                                                          err_message="Database Tender_list data insertion error. " +
                                                                      e.args[0])
            error_code = -3

    return error_code


def insert_Tender(db_connection, db_cursor, json_data, Tender_list_internal_ID):
    error_code = 0

    Tender_internal_ID = 0

    title = None
    description = None
    auctionUrl = None
    value_amount = None
    value_currency = None
    value_valueAddedTaxIncluded = None
    owner = None
    date = None
    dateCreated = None
    dateModified = None
    status = None
    submissionMethod = None
    procurementMethod = None
    mainProcurementCategory = None
    procuringEntity_internal_ID = None
    enquiryPeriod_startDate = None
    enquiryPeriod_endDate = None
    enquiryPeriod_clarificationsUntil = None
    enquiryPeriod_invalidationDate = None
    tenderPeriod_startDate = None
    tenderPeriod_endDate = None
    complaintPeriod_startDate = None
    complaintPeriod_endDate = None
    auctionPeriod_startDate = None
    auctionPeriod_endDate = None
    procuringEntity_result = 0
    
    if "title" in json_data:
        title=json_data["title"]
    if "description" in json_data:
        description=json_data["description"]
    if "auctionUrl" in json_data:
        auctionUrl=json_data["auctionUrl"]
    if "value" in json_data:
        if "amount" in json_data["value"]:
            value_amount=json_data["value"]["amount"]
        if "currency" in json_data["value"]:
            value_currency=json_data["value"]["currency"]
        if "valueAddedTaxIncluded" in json_data["value"]:
            value_valueAddedTaxIncluded=json_data["value"]["valueAddedTaxIncluded"]
    if "owner" in json_data:
        owner=json_data["owner"]
    if "date" in json_data:
        date=json_data["date"]
    if "dateCreated" in json_data:
        dateCreated=json_data["dateCreated"]
    if "dateModified" in json_data:
        dateModified=json_data["dateModified"]
    if "status" in json_data:
        status=json_data["status"]
    if "submissionMethod" in json_data:
        submissionMethod=json_data["submissionMethod"]
    if "procurementMethod" in json_data:
        procurementMethod=json_data["procurementMethod"]
    if "mainProcurementCategory" in json_data:
        mainProcurementCategory=json_data["mainProcurementCategory"]
    if "procuringEntity" in json_data:
        procuringEntity_result=insert_update_procuringEntity(db_connection=db_connection, db_cursor=db_cursor,
                                                    json_data_procuringEntity=json_data["procuringEntity"])
        if procuringEntity_result<0:
            error_code = procuringEntity_result
        elif procuringEntity_result>0:
            procuringEntity_internal_ID = procuringEntity_result
    if "enquiryPeriod" in json_data:
        if "startDate" in json_data["enquiryPeriod"]:
            enquiryPeriod_startDate=json_data["enquiryPeriod"]["startDate"]
        if "endDate" in json_data["enquiryPeriod"]:
            enquiryPeriod_endDate=json_data["enquiryPeriod"]["endDate"]
        if "clarificationsUntil" in json_data["enquiryPeriod"]:
            enquiryPeriod_clarificationsUntil=json_data["enquiryPeriod"]["clarificationsUntil"]
        if "invalidationDate" in json_data["enquiryPeriod"]:
            enquiryPeriod_invalidationDate=json_data["enquiryPeriod"]["invalidationDate"]
    if "tenderPeriod" in json_data:
        if "startDate" in json_data["tenderPeriod"]:
            tenderPeriod_startDate=json_data["tenderPeriod"]["startDate"]
        if "endDate" in json_data["tenderPeriod"]:
            tenderPeriod_endDate=json_data["tenderPeriod"]["endDate"]
    if "complaintPeriod" in json_data:
        if "startDate" in json_data["complaintPeriod"]:
            complaintPeriod_startDate=json_data["complaintPeriod"]["startDate"]
        if "endDate" in json_data["complaintPeriod"]:
            complaintPeriod_endDate=json_data["complaintPeriod"]["endDate"]
    if "auctionPeriod" in json_data:
        if "startDate" in json_data["auctionPeriod"]:
            auctionPeriod_startDate=json_data["auctionPeriod"]["startDate"]
        if "endDate" in json_data["auctionPeriod"]:
            auctionPeriod_endDate=json_data["auctionPeriod"]["endDate"]

    if error_code==0:
        statement = """INSERT INTO Tender (Tender_list_internal_ID, title, description, auctionUrl, value_amount, 
                value_currency, value_valueAddedTaxIncluded, owner, date, dateCreated, dateModified, status, 
                submissionMethod, procurementMethod, mainProcurementCategory, procuringEntity_internal_ID, 
                enquiryPeriod_startDate, enquiryPeriod_endDate, enquiryPeriod_clarificationsUntil, 
                enquiryPeriod_invalidationDate, tenderPeriod_startDate, tenderPeriod_endDate, 
                complaintPeriod_startDate, complaintPeriod_endDate, auctionPeriod_startDate, auctionPeriod_endDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """
    
        value = (Tender_list_internal_ID, title, description, auctionUrl, value_amount, value_currency,
                 value_valueAddedTaxIncluded, owner,
                 date, dateCreated, dateModified, status, submissionMethod, procurementMethod,mainProcurementCategory,
                 procuringEntity_internal_ID, enquiryPeriod_startDate, enquiryPeriod_endDate,
                 enquiryPeriod_clarificationsUntil, enquiryPeriod_invalidationDate, tenderPeriod_startDate,
                 tenderPeriod_endDate, complaintPeriod_startDate, complaintPeriod_endDate,auctionPeriod_startDate,
                 auctionPeriod_endDate)
    
        try:
            db_cursor.execute(statement, value)
            db_connection.commit()
    
            q_statement = """ SELECT internal_ID 
                            FROM Tender
                            ORDER BY internal_ID DESC LIMIT 1
                            """
    
            db_cursor.execute(q_statement)
            Tender_internal_ID = db_cursor.fetchone()[0]
    
            if "items" in json_data:
                insert_items(db_connection=db_connection, db_cursor=db_cursor,
                             json_data_items=json_data["items"], Tender_internal_ID=Tender_internal_ID)
    
    
        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                          err_module="Prozorro Local DB",
                                                          err_message="Database Tender data insertion error. " +
                                                                      e.args[0])
            error_code = -3

    return Tender_internal_ID, error_code


def insert_update_procuringEntity(db_connection, db_cursor, json_data_procuringEntity):
    error_code = 0
    result = 0
    procuringEntityId = 0

    name = None 
    name_en = None 
    identifier_scheme = None 
    identifier_id = None 
    identifier_legalName = None
    identifier_legalName_en = None 
    address_postalCode = None 
    address_countryName = None 
    address_region = None 
    address_locality = None
    address_streetAddress = None 
    contactPoint_name = None 
    contactPoint_telephone = None 
    contactPoint_email = None
    contactPoint_url = None 
    kind = None

    if "name" in json_data_procuringEntity:
        name = json_data_procuringEntity["name"]
    if "name_en" in json_data_procuringEntity:
        name_en = json_data_procuringEntity["name_en"]
    if "identifier" in json_data_procuringEntity:
        if "scheme" in json_data_procuringEntity["identifier"]:
            identifier_scheme = json_data_procuringEntity["identifier"]["scheme"]
        if "id" in json_data_procuringEntity["identifier"]:
            identifier_id = json_data_procuringEntity["identifier"]["id"]
        if "legalName" in json_data_procuringEntity["identifier"]:
            identifier_legalName = json_data_procuringEntity["identifier"]["legalName"]
        if "legalName_en" in json_data_procuringEntity["identifier"]:
            identifier_legalName_en = json_data_procuringEntity["identifier"]["legalName_en"]
    if "address" in json_data_procuringEntity:
        if "postalCode" in json_data_procuringEntity["address"]:
            address_postalCode = json_data_procuringEntity["address"]["postalCode"]
        if "countryName" in json_data_procuringEntity["address"]:
            address_countryName = json_data_procuringEntity["address"]["countryName"]
        if "region" in json_data_procuringEntity["address"]:
            address_region = json_data_procuringEntity["address"]["region"]
        if "locality" in json_data_procuringEntity["address"]:
            address_locality = json_data_procuringEntity["address"]["locality"]
        if "streetAddress" in json_data_procuringEntity["address"]:
            address_streetAddress = json_data_procuringEntity["address"]["streetAddress"]
    if "contactPoint" in json_data_procuringEntity:
        if "name" in json_data_procuringEntity["contactPoint"]:
            contactPoint_name = json_data_procuringEntity["contactPoint"]["name"]
        if "telephone" in json_data_procuringEntity["contactPoint"]:
            contactPoint_telephone = json_data_procuringEntity["contactPoint"]["telephone"]
        if "email" in json_data_procuringEntity["contactPoint"]:
            contactPoint_email = json_data_procuringEntity["contactPoint"]["email"]
        if "url" in json_data_procuringEntity["contactPoint"]:
            contactPoint_url = json_data_procuringEntity["contactPoint"]["url"]
    if "kind" in json_data_procuringEntity:
        kind = json_data_procuringEntity["kind"]

    q_statement = """SELECT internal_ID 
                    FROM procuringEntity 
                    WHERE identifier_scheme = ? AND 
                    identifier_id = ?
                    """

    q_value = (identifier_scheme, identifier_id)

    db_cursor.execute(q_statement, q_value)
    fetch_result = db_cursor.fetchall()
    if len(fetch_result)==0:
        statement = """INSERT INTO procuringEntity (name, name_en, identifier_scheme, identifier_id, identifier_legalName, 
                identifier_legalName_en, address_postalCode, address_countryName, address_region, address_locality, 
                address_streetAddress, contactPoint_name, contactPoint_telephone, contactPoint_email, 
                contactPoint_url, kind)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """


        value = ( name, name_en, identifier_scheme, identifier_id, identifier_legalName, identifier_legalName_en,
                  address_postalCode, address_countryName, address_region,address_locality, address_streetAddress,
                  contactPoint_name, contactPoint_telephone, contactPoint_email, contactPoint_url, kind)

        try:
            db_cursor.execute(statement, value)
            db_connection.commit()
        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                          err_module="Prozorro Local DB",
                                                          err_message="Database procuringEntity data insertion error." +
                                                                      e.args[0])
            error_code = -5

    else:
        statement = """UPDATE procuringEntity 
                    SET name = ?, name_en  = ?, identifier_legalName = ?, identifier_legalName_en = ?, 
                    address_postalCode = ?, address_countryName = ?, address_region = ?, address_locality = ?,
                    address_streetAddress = ?, contactPoint_name = ?, contactPoint_telephone = ?, 
                    contactPoint_email = ?, contactPoint_url = ?, kind = ?
                    WHERE identifier_scheme = ? AND identifier_id = ?; """

        value = (name, name_en, identifier_legalName, identifier_legalName_en,
                 address_postalCode, address_countryName, address_region, address_locality, address_streetAddress,
                 contactPoint_name, contactPoint_telephone, contactPoint_email, contactPoint_url, kind,
                 identifier_scheme, identifier_id)

        try:
            db_cursor.execute(statement, value)
            db_connection.commit()
        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                          err_module="Prozorro Local DB",
                                                          err_message="Database procuringEntity data updating error." +
                                                                      e.args[0])
            error_code = -15

    try:
        db_cursor.execute(q_statement, q_value)
        fetch_result = db_cursor.fetchall()
        if len(fetch_result)==0:
            error_code = -26
        else:
            procuringEntityId = fetch_result[0][0]

    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                      err_module="Prozorro Local DB",
                                                      err_message="Database procuringEntity data retrieving error." +
                                                                  e.args[0])
        error_code = -25

    if error_code<0:
        result=error_code
    else:
        result=procuringEntityId

    return result


def insert_items(db_connection, db_cursor, json_data_items, Tender_internal_ID):
    error_code = 0
    id = None
    description = None
    classification_scheme = None
    classification_id = None
    classification_description = None
    quantity = None
    unit_name = None
    unit_code = None
    deliveryDate_startDate = None
    deliveryDate_endDate = None
    deliveryAddress_postalCode = None
    deliveryAddress_countryName = None
    deliveryAddress_region = None
    deliveryAddress_locality = None
    deliveryAddress_streetAddress = None
    deliveryLocation_latitude = None
    deliveryLocation_longitude = None
    deliveryLocation_elevation = None

    for item in json_data_items:
        if "id" in item:
            id = item["id"]
        if "description" in item:
            description = item["description"]
        if "classification" in item:
            if "scheme" in item["classification"]:
                classification_scheme = item["classification"]["scheme"]
            if "id" in item["classification"]:
                classification_id = item["classification"]["id"]
            if "description" in item["classification"]:
                classification_description = item["classification"]["description"]
        if "quantity" in item:
            quantity = item["quantity"]
        if "unit" in item:
            if "code" in item["unit"]:
                unit_code = item["unit"]["code"]
            if "name" in item["unit"]:
                unit_name = item["unit"]["name"]
        if "deliveryDate" in item:
            if "startDate" in item["deliveryDate"]:
                deliveryDate_startDate = item["deliveryDate"]["startDate"]
            if "endDate" in item["deliveryDate"]:
                deliveryDate_endDate = item["deliveryDate"]["endDate"]
        if "deliveryAddress" in item:
            if "postalCode" in item["deliveryAddress"]:
                deliveryAddress_postalCode = item["deliveryAddress"]["postalCode"]
            if "countryName" in item["deliveryAddress"]:
                deliveryAddress_countryName = item["deliveryAddress"]["countryName"]
            if "region" in item["deliveryAddress"]:
                deliveryAddress_region = item["deliveryAddress"]["region"]
            if "locality" in item["deliveryAddress"]:
                deliveryAddress_locality = item["deliveryAddress"]["locality"]
            if "streetAddress" in item["deliveryAddress"]:
                deliveryAddress_streetAddress = item["deliveryAddress"]["streetAddress"]
        if "deliveryLocation" in item:
            if "latitude" in item["deliveryLocation"]:
                deliveryLocation_latitude = item["deliveryLocation"]["latitude"]
            if "longitude" in item["deliveryLocation"]:
                deliveryLocation_longitude = item["deliveryLocation"]["longitude"]
            if "elevation" in item["deliveryLocation"]:
                deliveryLocation_elevation = item["deliveryLocation"]["elevation"]

        statement = """INSERT INTO items (id, description, classification_scheme, classification_id, 
                classification_description, quantity, unit_name, unit_code, deliveryDate_startDate, 
                deliveryDate_endDate, deliveryAddress_postalCode, deliveryAddress_countryName, 
                deliveryAddress_region, deliveryAddress_locality, deliveryAddress_streetAddress, 
                deliveryLocation_latitude, deliveryLocation_longitude, deliveryLocation_elevation, 
                Tender_internal_ID)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """

        value = (id, description, classification_scheme, classification_id, classification_description, quantity,
                 unit_name, unit_code, deliveryDate_startDate, deliveryDate_endDate, deliveryAddress_postalCode,
                 deliveryAddress_countryName, deliveryAddress_region, deliveryAddress_locality,
                 deliveryAddress_streetAddress, deliveryLocation_latitude, deliveryLocation_longitude,
                 deliveryLocation_elevation, Tender_internal_ID)


        try:
            db_cursor.execute(statement, value)
            db_connection.commit()
        except sqlite3.Error as e:
            print(f"SQLite error {e.args[0]}")
            telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                          err_module="Prozorro Local DB",
                                                          err_message="Database items data insertion error." +
                                                                      e.args[0])
            error_code = -7


    return error_code
