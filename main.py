import asyncio
import configparser
from datetime import datetime
import json
import logging
import logging.handlers
import requests
import os

from iotc import IOTCConnectType, IOTCEvents, IOTCLogLevel
from iotc.aio import IoTCClient
from iotc.models import Command, Property

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "machines.ini"))

class FileLogger:
    def __init__(self, logpath, logname="iotc_py_log"):
        self._logger = logging.getLogger(logname)
        self._logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(
            os.path.join(logpath, logname), maxBytes=20000, backupCount=5)
        self._logger.addHandler(handler)

    async def _log(self, message):
        print(message)
        self._logger.debug(message)

    async def info(self, message):
        if self._log_level != IOTCLogLevel.IOTC_LOGGING_DISABLED:
            await self._log(message)

    async def debug(self, message):
        if self._log_level == IOTCLogLevel.IOTC_LOGGING_ALL:
            await self._log(message)

    def set_log_level(self, log_level):
        self._log_level = log_level


current_bactosense = "BACTO910107"
device_id = config[current_bactosense]["DeviceId"]
scope_id = config[current_bactosense]["ScopeId"]
key = config[current_bactosense]["SasKey"]
hub_name = "bactosense"
data_filename = "last_data.json"

async def on_props(prop: Property):
    print(f"Received {prop.name}:{prop.value}")
    return True


async def on_commands(command: Command):
    print("Received command {} with value {}".format(command.name, command.value))
    await command.reply()


async def on_enqueued_commands(command: Command):
    print("Received offline command {} with value {}".format(
        command.name, command.value))


# change connect type to reflect the used key (device or group)
client = IoTCClient(
    device_id,
    scope_id,
    IOTCConnectType.IOTC_CONNECT_SYMM_KEY,
    key,
    logger=FileLogger("."),
)

client.set_log_level(IOTCLogLevel.IOTC_LOGGING_ALL)
client.on(IOTCEvents.IOTC_PROPERTIES, on_props)
client.on(IOTCEvents.IOTC_COMMAND, on_commands)
client.on(IOTCEvents.IOTC_ENQUEUED_COMMAND, on_enqueued_commands)


def get_telemetry_from_bactosense(ip = config[current_bactosense]["Ip"]):
    resp = requests.get("http://"+ip+"/data/auto/last", auth=('service', '0603'))
    resp = resp.json()
    data = {}

    fields = {
        'Timestamp': 'timestamp',
        'ICC': 'ICC',
        'TCC': 'TCC',
        'HNAP': 'HNAP',
        'Date': 'date',
        'UTCDate': 'dateUtc',
    }

    for key, value in fields.items():
        if value in resp:
            data[key] = resp[value]
            
    return data

def get_properties_from_bactosense(ip = config[current_bactosense]["Ip"]):
    resp = requests.get("http://"+ip+"/api/status", auth=('service', '0603'))
    resp = resp.json()
    data = {}

    fields = {
        'CartridgeLevel': 'cartridgeLevel',
        'Version': 'version',
        'CartridgeExpiry': 'cartridgeExpiry',
        'DiskMeasurementsRemaining': 'diskMeasurementsRemaining',
        'PumpMotions': 'pumpMotions',
        'PlungerMotions':"plungerMotions",
        'ValveMotions': "valveMotions",
        'MixerMotions': 'mixerMotions',
        'CartridgeSerial': 'cartridgeSerial',
        'SerialNumber': "serialNumber",
        'NextServiceDue': 'nextServiceDue',
        'Temperature': 'temperature',
    }

    for key, value in fields.items():
        if value in resp:
            data[key] = resp[value]
    
    if 'NextServiceDue' in data:
        data['NextServiceDue'] = datetime.fromtimestamp(int(data['NextServiceDue'])).isoformat()

    if 'CartridgeExpiry' in data:
        data['CartridgeExpiry'] = datetime.fromtimestamp(int(data['CartridgeExpiry'])).isoformat()

    return data

async def main():
    try:
        last_data = json.load(open(data_filename, 'r'))
    except:
        last_data = {}

    await client.connect()
    
    while not client.terminated():
        data = get_telemetry_from_bactosense()
        props = get_properties_from_bactosense()

        if data != last_data:
            msg_prop = {
                'iothub-creation-time-utc' : data['UTCDate']
            }
            await client.send_telemetry(data, properties=msg_prop)
            await client.send_property(props)
            last_data = data
            json.dump(last_data, open(data_filename, 'w'))
        else:
            await client._logger.info("No new data, waiting...")

        await asyncio.sleep(30)

asyncio.run(main())
