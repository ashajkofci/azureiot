import asyncio
import os
import uuid

from azure.iot.device import Message
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device.aio import IoTHubDeviceClient, ProvisioningDeviceClient

messages_to_send = 10

PRIMARY_KEY = "mBUTofp4+gMVYZ5JivCRgNICjdbQOd4A1e/u76Ot/P8="
DEVICE_ID = "bacto118"
ID_SCOPE = "0ne0075E69E"

data = {    
        "Water_Quality_Monitor_79g": {
            "Temperature": 78.50918337376584,
            "Conductivity": 22.537619243941425,
            "Turbidity": 34.04916105902673,
            "Salinity": 73.72618747652545,
            "AcidityPH": -41.28777605279629
        },
        "Water_Quality_Monitor_2nj": {
            "Ammonium": 81.86262755139609,
            "Ammonia": 69.52489872401405,
            "Chloride": 32.97931322181711,
            "Nitrate": 6.380287875061088,
            "Sodium": 66.96983463962808
        },
        "Water_Quality_Monitor_4yq": {
            "Location": {
                "lon": -96.1267,
                "lat": 36.3185,
                "alt": 1326.8035
            }
        }
    }
async def main():

    # The connection string for a device should never be stored in code. For the sake of simplicity we're using an environment variable here.
    provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
        provisioning_host='global.azure-devices-provisioning.net',
        registration_id=DEVICE_ID,
        id_scope=ID_SCOPE,
        symmetric_key=PRIMARY_KEY)
    registration_result = await provisioning_device_client.register()

    # Build the connection string - this is used to connect to IoT Central
    conn_str = 'HostName=' + registration_result.registration_state.assigned_hub + \
                ';DeviceId=' + DEVICE_ID + \
                ';SharedAccessKey=' + PRIMARY_KEY

    # The client object is used to interact with your Azure IoT hub.
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)

    # Connect the client.
    await device_client.connect()

    async def send_test_message(i):
        print("sending message #" + str(i))
        msg = Message("test wind speed " + str(i))
        msg.message_id = uuid.uuid4()
        #msg.correlation_id = "correlation-1234"
        msg.custom_properties = data
        msg.content_encoding = "utf-8"
        msg.content_type = "application/json"
        await device_client.send_message(msg)
        print("done sending message #" + str(i))

    # send `messages_to_send` messages in parallel
    await asyncio.gather(*[send_test_message(i) for i in range(1, messages_to_send + 1)])

    # Finally, shut down the client
    await device_client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())