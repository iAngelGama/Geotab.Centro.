import requests
import pandas as pd
from datetime import datetime
from pytz import timezone
from google.colab import auth
import gspread
from gspread_dataframe import set_with_dataframe
from google.auth import default

server = "https://my.geotab.com"
username = "daira.avilalara@mercadolibre.com.mx"
password = "DGEM3006"
database = "melimlm"

RULE_MAP = {
    "acD-1ae1xTkODSthI_-zH7A": "Exceso de Velocidad",
    "avaXGswL8pU-PfmJr5VdOSw": "Hard Acceleration",
    "aDp-8sj2cyUKKkoKmeneVug": "Harsh Braking",
    "awuLHEieBXkypMeyX4_rkyw": "Harsh Cornering",
    "RuleEnhancedMajorCollisionId": "Major Collision",
    "aU5XTqBqIF0m5YzsvjkucBQ": "Possible Collision",
    "aFYQCyFpV2EyqZvLkqE8ehg": "Uso del Cinturón",
}

def authenticate():
    url = f"{server}/apiv1"
    payload = {
        "method": "Authenticate",
        "params": {
            "userName": username,
            "password": password,
            "database": database,
        },
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["result"]["credentials"]

def get_data(credentials, method, params):
    url = f"{server}/apiv1"
    payload = {
        "method": method,
        "params": {**params, "credentials": credentials},
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["result"]

def adjust_to_utc_today(local_zone):
    local_tz = timezone(local_zone)
    today_local = datetime.now(local_tz)
    start_time_local = today_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time_local = today_local.replace(hour=23, minute=59, second=59, microsecond=0)
    return start_time_local.astimezone(timezone("UTC")).isoformat(), end_time_local.astimezone(timezone("UTC")).isoformat()

def generate_and_upload_report(start_time, end_time):
    exceptions, trips, devices = fetch_report_data(start_time, end_time)

    print("Primeros 5 registros de exceptions:", exceptions[:5])
    print("Primeros 5 registros de trips:", trips[:5])
    print("Primeros 5 registros de devices:", devices[:5])

    device_map = {device["id"]: device.get("name", "Unknown") for device in devices}

    all_devices_df = pd.DataFrame({"deviceName": list(device_map.values())})
    print("Todos los dispositivos (primeros 5):", all_devices_df.head())

    exception_df = pd.DataFrame(exceptions)
    if not exception_df.empty:
        exception_df["deviceName"] = exception_df["device"].apply(lambda x: device_map.get(x.get("id", ""), "Unknown"))
        exception_df["ruleName"] = exception_df["rule"].apply(lambda x: RULE_MAP.get(x.get("id", ""), None))
        exception_df = exception_df[~exception_df["ruleName"].isnull()]

        print("Excepciones procesadas (primeros 5):", exception_df.head())

        exception_summary = exception_df.groupby(["deviceName", "ruleName"]).size().unstack(fill_value=0).reset_index()

        print("Resumen de excepciones agrupadas:", exception_summary.head())
    else:
        print("No se encontraron excepciones.")
        exception_summary = all_devices_df.copy()
        for rule in RULE_MAP.values():
            exception_summary[rule] = 0

    trip_df = pd.DataFrame(trips)
    if not trip_df.empty:
        trip_df["deviceName"] = trip_df["device"].apply(lambda x: device_map.get(x.get("id", ""), "Unknown"))
        trip_summary = trip_df.groupby("deviceName")["distance"].sum().reset_index(name="total_kilometers")

        print("Resumen de viajes agrupados:", trip_summary.head())
    else:
        print("No se encontraron viajes.")
        trip_summary = pd.DataFrame({"deviceName": list(device_map.values()), "total_kilometers": 0})

    report_df = pd.merge(all_devices_df, exception_summary, on="deviceName", how="left").fillna(0)
    report_df = pd.merge(report_df, trip_summary, on="deviceName", how="left").fillna(0)

    columns_order = [
        "deviceName", "Exceso de Velocidad", "Hard Acceleration", "Harsh Braking",
        "Harsh Cornering", "Major Collision", "Possible Collision",
        "Uso del Cinturón", "total_kilometers"
    ]
    for col in columns_order:
        if col not in report_df.columns:
            report_df[col] = 0

    report_df = report_df[columns_order]

    print("Reporte final (primeros 5 registros):", report_df.head())

    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)

    spreadsheet_name = "Base Centro"
    sheet = gc.open(spreadsheet_name)
    worksheet = sheet.worksheet("Geotab+")

    worksheet.batch_clear(["A:I"])

    set_with_dataframe(worksheet, report_df, include_index=False, include_column_header=True)
    print("Reporte subido a Google Sheets con éxito.")
    
def clean_column_a(worksheet):
    column_a_values = worksheet.col_values(1)
    cleaned_values = [[value.rstrip()] for value in column_a_values] 
    worksheet.update("A1:A" + str(len(cleaned_values)), cleaned_values)
    print("Espacios finales en la columna A eliminados.")

def fetch_report_data(start_time, end_time):
    credentials = authenticate()

    exception_params = {
        "typeName": "ExceptionEvent",
        "search": {
            "fromDate": start_time,
            "toDate": end_time,
        },
    }
    exceptions = get_data(credentials, "Get", exception_params)

    trip_params = {
        "typeName": "Trip",
        "search": {
            "fromDate": start_time,
            "toDate": end_time,
        },
    }
    trips = get_data(credentials, "Get", trip_params)

    device_params = {
        "typeName": "Device",
    }
    devices = get_data(credentials, "Get", device_params)

    return exceptions, trips, devices


zone = "America/Mexico_City"
start_time, end_time = adjust_to_utc_today(zone)

try:
    generate_and_upload_report(start_time, end_time)
except Exception as e:
    print(f"Error generando o subiendo el reporte: {e}")

server = "https://my.geotab.com"
username = "daira.avilalara@mercadolibre.com.mx"
password = "DGEM3006"
database = "mercadolibre"

RULE_MAP = {
    "aJIhq0JHMLkayJPKLNzPktQ": "Exceso de Velocidad",
    "a29UNa2PMwkyXkBi__xFUlQ": "Hard Acceleration",
    "aiFwEqtRB4kePQbw-uLtY6A": "Harsh Braking",
    "alIhR1prk702ODgzaZ5xMgg": "Harsh Cornering",
    "RuleEnhancedMajorCollisionId": "Major Collision",
    "RuleEnhancedMinorCollisionId": "Possible Collision",
    "a-RRLPZCHVUe9s-WU5OPizw": "Uso del Cinturón",
}

def authenticate():
    url = f"{server}/apiv1"
    payload = {
        "method": "Authenticate",
        "params": {
            "userName": username,
            "password": password,
            "database": database,
        },
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["result"]["credentials"]

def get_data(credentials, method, params):
    url = f"{server}/apiv1"
    payload = {
        "method": method,
        "params": {**params, "credentials": credentials},
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["result"]

def adjust_to_utc_today(local_zone):
    local_tz = timezone(local_zone)
    today_local = datetime.now(local_tz)
    start_time_local = today_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time_local = today_local.replace(hour=23, minute=59, second=59, microsecond=0)
    return start_time_local.astimezone(timezone("UTC")).isoformat(), end_time_local.astimezone(timezone("UTC")).isoformat()

def generate_and_upload_report(start_time, end_time):
    exceptions, trips, devices = fetch_report_data(start_time, end_time)

    print("Primeros 5 registros de exceptions:", exceptions[:5])
    print("Primeros 5 registros de trips:", trips[:5])
    print("Primeros 5 registros de devices:", devices[:5])

    device_map = {device["id"]: device.get("name", "Unknown") for device in devices}

    all_devices_df = pd.DataFrame({"deviceName": list(device_map.values())})
    print("Todos los dispositivos (primeros 5):", all_devices_df.head())

    exception_df = pd.DataFrame(exceptions)
    if not exception_df.empty:
        exception_df["deviceName"] = exception_df["device"].apply(lambda x: device_map.get(x.get("id", ""), "Unknown"))
        exception_df["ruleName"] = exception_df["rule"].apply(lambda x: RULE_MAP.get(x.get("id", ""), None))
        exception_df = exception_df[~exception_df["ruleName"].isnull()]

        print("Excepciones procesadas (primeros 5):", exception_df.head())

        exception_summary = exception_df.groupby(["deviceName", "ruleName"]).size().unstack(fill_value=0).reset_index()

        print("Resumen de excepciones agrupadas:", exception_summary.head())
    else:
        print("No se encontraron excepciones.")
        exception_summary = all_devices_df.copy()
        for rule in RULE_MAP.values():
            exception_summary[rule] = 0

    trip_df = pd.DataFrame(trips)
    if not trip_df.empty:
        trip_df["deviceName"] = trip_df["device"].apply(lambda x: device_map.get(x.get("id", ""), "Unknown"))
        trip_summary = trip_df.groupby("deviceName")["distance"].sum().reset_index(name="total_kilometers")

        print("Resumen de viajes agrupados:", trip_summary.head())
    else:
        print("No se encontraron viajes.")
        trip_summary = pd.DataFrame({"deviceName": list(device_map.values()), "total_kilometers": 0})

    report_df = pd.merge(all_devices_df, exception_summary, on="deviceName", how="left").fillna(0)
    report_df = pd.merge(report_df, trip_summary, on="deviceName", how="left").fillna(0)

    columns_order = [
        "deviceName", "Exceso de Velocidad", "Hard Acceleration", "Harsh Braking",
        "Harsh Cornering", "Major Collision", "Possible Collision",
        "Uso del Cinturón", "total_kilometers"
    ]
    for col in columns_order:
        if col not in report_df.columns:
            report_df[col] = 0

    report_df = report_df[columns_order]

    print("Reporte final (primeros 5 registros):", report_df.head())

    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)

    spreadsheet_name = "Base Centro"
    sheet = gc.open(spreadsheet_name)
    worksheet = sheet.worksheet("Geotab+")

    existing_rows = len(worksheet.get_all_values())
    start_row = existing_rows + 1 if existing_rows > 0 else 1

    set_with_dataframe(worksheet, report_df, row=start_row, include_index=False, include_column_header=(start_row == 1))
    print(f"Reporte subido a Google Sheets desde la fila {start_row} con éxito.")
    
def clean_column_a(worksheet):
    column_a_values = worksheet.col_values(1)
    cleaned_values = [[value.rstrip()] for value in column_a_values] 
    worksheet.update("A1:A" + str(len(cleaned_values)), cleaned_values)
    print("Espacios finales en la columna A eliminados.")

def fetch_report_data(start_time, end_time):
    credentials = authenticate()

    exception_params = {
        "typeName": "ExceptionEvent",
        "search": {
            "fromDate": start_time,
            "toDate": end_time,
        },
    }
    exceptions = get_data(credentials, "Get", exception_params)

    trip_params = {
        "typeName": "Trip",
        "search": {
            "fromDate": start_time,
            "toDate": end_time,
        },
    }
    trips = get_data(credentials, "Get", trip_params)

    device_params = {
        "typeName": "Device",
    }
    devices = get_data(credentials, "Get", device_params)

    return exceptions, trips, devices

zone = "America/Mexico_City"
start_time, end_time = adjust_to_utc_today(zone)

try:
    generate_and_upload_report(start_time, end_time)
except Exception as e:
    print(f"Error generando o subiendo el reporte: {e}")


