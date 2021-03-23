#!/usr/bin/env python3
from influxdb_client.client.write.point import DEFAULT_WRITE_PRECISION
from influxdb_client.domain.write_precision import WritePrecision
import requests
import json
import yaml
from timeloop import Timeloop
from datetime import timedelta, datetime
import logging


from influxdb_client import InfluxDBClient, WriteApi, WriteOptions, Point
from influxdb_client.client.write_api import SYNCHRONOUS

tl = Timeloop()
_last_timestamp = None
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


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


def write_data(points, _db_client, bucket):
    write_api = _db_client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=bucket, record=points)


@tl.job(interval=timedelta(seconds=2))
def polling_loop():
    try:
        logging.info("starting polling loop...")
        _db_client = InfluxDBClient(
            url=cfg["influxdb"]["url"], 
            token=cfg["influxdb"]["token"], 
            org=cfg["influxdb"]["org"], 
            debug=False)
        points = fetch_inverter_data(cfg["inverter"]["livedata_url"], 
                                     cfg["inverter"]["serial_number"],
                                     cfg["inverter"]["auth"]["username"],
                                     cfg["inverter"]["auth"]["password"])
        if points != None:
            logging.info("writing point to InfluxDB...")
            write_data(points, _db_client, cfg["influxdb"]["bucket"])
            _db_client.close()
    except Exception:
        logging.exception("Exception in main polling loop")


if __name__ == "__main__":
    with open("config.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.BaseLoader)
    tl.start(block=True)
