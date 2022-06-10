#!/usr/bin/env python3
from influxdb_client.client.write.point import DEFAULT_WRITE_PRECISION
from influxdb_client.domain.write_precision import WritePrecision
import requests
import json
import yaml
import atexit
from timeloop import Timeloop
from datetime import timedelta, datetime
import logging
from suntime import Sun, SunTimeException

from influxdb_client import InfluxDBClient, Point, WriteApi
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS

tl = Timeloop()
_last_timestamp = None

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

def on_exit(db_client: InfluxDBClient, write_api: WriteApi):
    """Close clients after terminate a script.

    :param db_client: InfluxDB client
    :param write_api: WriteApi
    :return: nothing
    """
    write_api.close()
    db_client.close()


def fetch_inverter_data(url: str, serial_number: str, username: str, password: str):
    """Fetch data from inverter livedata

    :return: livedata json
    """

    global _last_timestamp

    try:
        response = requests.get(url, auth=(username, password), timeout=3)
        if response.status_code == 200:
            livedata = json.loads(response.text)
            timestamp = datetime.strptime(
                livedata[serial_number]["timestamp"], "%Y-%m-%dT%H:%M:%S%z")
            if timestamp != _last_timestamp:
                points = livedata[serial_number]["points"]
                for point in points:
                    yield Point.measurement("FV_measurement").field(point["name"], point["value"]).time(timestamp, write_precision=WritePrecision.S)
                _last_timestamp = timestamp
            else:
                logging.warning("Same timestamp, skipping...")
                return None
        else:
            logging.error("Inverter response not OK")
    except Exception:
        logging.exception("No inverter connection?")
        return None


def write_data(points, bucket):
    _write_api.write(bucket=bucket, record=points)

def get_sun_times():
    latitude=cfg["inverter"]["location"]["latitude"]
    longitude=cfg["inverter"]["location"]["longitude"]
    delta_minutes=cfg["inverter"]["location"]["delta_minutes"]
    sun = Sun(latitude, longitude)
    today_sr = sun.get_local_sunrise_time() - timedelta(minutes=delta_minutes)
    today_ss = sun.get_local_sunset_time() + timedelta(minutes=delta_minutes)
    return today_sr, today_ss

def is_suntime():
    today_sr, today_ss = get_sun_times()
    return (today_sr < datetime.now(today_sr.tzinfo) < today_ss)

@tl.job(interval=timedelta(seconds=2))
def polling_loop():
    try:
        if (is_suntime()):
            logging.info("starting polling loop...")
            points = fetch_inverter_data(cfg["inverter"]["livedata_url"], 
                                        cfg["inverter"]["serial_number"],
                                        cfg["inverter"]["auth"]["username"],
                                        cfg["inverter"]["auth"]["password"])
            if points != None:
                logging.info("writing point to InfluxDB...")
                write_data(points, cfg["influxdb"]["bucket"])
        else:
            logging.info("night time...")
    except Exception:
        logging.exception("Exception in main polling loop")


if __name__ == "__main__":
    with open("config.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)

    _db_client = InfluxDBClient(
        url=cfg["influxdb"]["url"], 
        token=cfg["influxdb"]["token"], 
        org=cfg["influxdb"]["org"], 
        debug=False)
    _write_api = _db_client.write_api(write_options=ASYNCHRONOUS)
    atexit.register(on_exit, _db_client, _write_api)
    tl.start(block=True)
    
