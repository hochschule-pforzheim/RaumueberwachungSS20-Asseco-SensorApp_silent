#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import time
import sys
from datetime import datetime
import logging as log
log.basicConfig(filename="app.log",filemode="a",level=log.INFO)
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue
from azure.iot.device.aio import IoTHubDeviceClient
from tinkerforge.ip_connection import IPConnection
from tinkerforge.ip_connection import Error

from tinkerforge.bricklet_ambient_light_v3 import BrickletAmbientLightV3
from tinkerforge.bricklet_barometer_v2 import BrickletBarometerV2
from tinkerforge.bricklet_air_quality import BrickletAirQuality
from tinkerforge.bricklet_motion_detector_v2 import BrickletMotionDetectorV2

import re

# HTTP options
TIMEOUT = 241000
MINIMUM_POLLING_TIME = 12

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

RECEIVE_CONTEXT = 0
MESSAGE_COUNT = 0
MESSAGE_SWITCH = True
TWIN_CONTEXT = 0
SEND_REPORTED_STATE_CONTEXT = 0
METHOD_CONTEXT = 0
TEMPERATURE_ALERT = 30.0

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
BLOB_CALLBACKS = 0
TWIN_CALLBACKS = 0
SEND_REPORTED_STATE_CALLBACKS = 0
METHOD_CALLBACKS = 0
EVENT_SUCCESS = "success"
EVENT_FAILED = "failed"

# chose HTTP, AMQP or MQTT as transport protocol
PROTOCOL = IoTHubTransportProvider.MQTT

#SENSOR DATA
HOST = "localhost"
PORT = 4223

#INITIALIZATION
ipcon = None
humidity = None
barometer = None
ambientLight = None
airQuality = None

MAX_VALUE = 9223372036854775807

#Enumeration
def cb_enumerate(uid, connected_uid, position, hardware_version,
                 firmware_version, device_identifier, enumeration_type):  
    global motionDetector
    global airQuality
    global ambientLight
    global barometer
    if enumeration_type == IPConnection.ENUMERATION_TYPE_CONNECTED or \
       enumeration_type == IPConnection.ENUMERATION_TYPE_AVAILABLE:
        if device_identifier == BrickletMotionDetectorV2.DEVICE_IDENTIFIER:
            try:
                motionDetector = BrickletMotionDetectorV2(uid, ipcon)
                log.info("Motion Detector init success")
            except Error as e:
                log.error("Motion Detector init failed")
        elif device_identifier == BrickletAmbientLightV3.DEVICE_IDENTIFIER:
            try:
                ambientLight = BrickletAmbientLightV3(uid, ipcon)
                log.info("AL init success")
            except Error as e:
                log.error("Ambient Light init failed")
        elif device_identifier == BrickletBarometerV2.DEVICE_IDENTIFIER:
            try:
                barometer = BrickletBarometerV2(uid, ipcon)
                log.info("Barometer init success")
                log.info(barometer.get_temperature())
            except Error as e:
                log.error("Barometer init failed")
        elif device_identifier == BrickletAirQuality.DEVICE_IDENTIFIER:
            try:
                airQuality = BrickletAirQuality(uid, ipcon)
                log.info("AQ init success")
            except Error as e:
                log.error("Air Quality init failed")   

def cb_connected(connected_reason):
    if connected_reason == IPConnection.CONNECT_REASON_AUTO_RECONNECT:
        log.info('Auto Reconnect')

        while True:
            try:
                ipcon.enumerate()
                break
            except Error as e:
                log.error('Enumerate Error: ' + str(e.description))
                time.sleep(1)

def is_correct_connection_string():
    m = re.search("HostName=.*;DeviceId=.*;", CONNECTION_STRING)
    if m:
        return True
    else:
        return False

CONNECTION_STRING = "HostName=RPIAssecoProjectHub.azure-devices.net;DeviceId=device01;SharedAccessKey=L4VGQg31/AZGpxYGPZInidS7ZrN8V8epBaX46GHbLMg=" 

if not is_correct_connection_string():
    log.error("Device connection string is not correct.")
    sys.exit(0)

#Dictionary init
MSG_TXT = {
    "PayLoadTimeStamp": 0,
    "ReverseTimeStamp": 0,
    "Payload": {
        "baro_temperature": 0,
        "baro_airpressure": 0,
        "baro_altitude": 0,
        "aq_iaq_index": 0,
        "aq_iaq_accuracy": 0,
        "aq_temperature": 0, 
        "aq_humidity": 0, 
        "aq_air_pressure": 0,              
        "al_illuminance": 0,
        "motion_detector": 0
    }
}

def receive_message_callback(message, counter):
    global RECEIVE_CALLBACKS
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    log.info("Received Message [%d]:" % counter )
    log.info("    Data: <<<%s>>> & Size=%d" % (message_buffer[:size].decode("utf-8"), size) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    log.info("    Properties: %s" % key_value_pair )
    counter += 1
    RECEIVE_CALLBACKS += 1
    log.info( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    return IoTHubMessageDispositionResult.ACCEPTED


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    log.info( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    log.info( "    message_id: %s" % message.message_id )
    log.info( "    correlation_id: %s" % message.correlation_id )
    key_value_pair = map_properties.get_internals()
    log.info( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    log.info( "    Total calls confirmed: %d" % SEND_CALLBACKS )


def device_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    log.info( "\nTwin callback called with:\nupdateStatus = %s\npayload = %s\ncontext = %s" % (update_state, payload, user_context) )
    TWIN_CALLBACKS += 1
    log.info( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


def send_reported_state_callback(status_code, user_context):
    global SEND_REPORTED_STATE_CALLBACKS
    log.info( "Confirmation for reported state received with:\nstatus_code = [%d]\ncontext = %s" % (status_code, user_context) )
    SEND_REPORTED_STATE_CALLBACKS += 1
    log.info( "    Total calls confirmed: %d" % SEND_REPORTED_STATE_CALLBACKS )


def device_method_callback(method_name, payload, user_context):
    global METHOD_CALLBACKS,MESSAGE_SWITCH
    log.info( "\nMethod callback called with:\nmethodName = %s\npayload = %s\ncontext = %s" % (method_name, payload, user_context) )
    METHOD_CALLBACKS += 1
    log.info( "Total calls confirmed: %d\n" % METHOD_CALLBACKS )
    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    device_method_return_value.status = 200
    if method_name == "start":
        MESSAGE_SWITCH = True
        log.info( "Start sending message\n" )
        device_method_return_value.response = "{ \"Response\": \"Successfully started\" }"
        return device_method_return_value
    if method_name == "stop":
        MESSAGE_SWITCH = False
        log.info( "Stop sending message\n" )
        device_method_return_value.response = "{ \"Response\": \"Successfully stopped\" }"
        return device_method_return_value
    return device_method_return_value


def blob_upload_conf_callback(result, user_context):
    global BLOB_CALLBACKS
    log.info( "Blob upload confirmation[%d] received for message with result = %s" % (user_context, result) )
    BLOB_CALLBACKS += 1
    log.info( "    Total calls confirmed: %d" % BLOB_CALLBACKS )


def iothub_client_init():
    # prepare iothub client
    client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
    client.set_option("product_info", "HappyPath_RaspberryPi-Python")
    if client.protocol == IoTHubTransportProvider.HTTP:
        client.set_option("timeout", TIMEOUT)
        client.set_option("MinimumPollingTime", MINIMUM_POLLING_TIME)
    # set the time until a message times out
    client.set_option("messageTimeout", MESSAGE_TIMEOUT)
    # to enable MQTT logging set to 1
    if client.protocol == IoTHubTransportProvider.MQTT:
        client.set_option("logtrace", 0)
    client.set_message_callback(
        receive_message_callback, RECEIVE_CONTEXT)
    if client.protocol == IoTHubTransportProvider.MQTT or client.protocol == IoTHubTransportProvider.MQTT_WS:
        client.set_device_twin_callback(
            device_twin_callback, TWIN_CONTEXT)
        client.set_device_method_callback(
            device_method_callback, METHOD_CONTEXT)
    return client


def print_last_message_time(client):
    try:
        last_message = client.get_last_message_receive_time()
        log.info( "Last Message: %s" % time.asctime(time.localtime(last_message)) )
        log.info( "Actual time : %s" % time.asctime() )
    except IoTHubClientError as iothub_client_error:
        if iothub_client_error.args[0].result == IoTHubClientResult.INDEFINITE_TIME:
            log.info("No message received")
        else:
            log.info("iothub_client_error")

def calculateTicks(dt):
    return (dt - datetime(1,1,1)).total_seconds()*10000000

def iothub_client_run():    
    try:
        client = iothub_client_init()
        
        if client.protocol == IoTHubTransportProvider.MQTT:
            log.info( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\"}"
            client.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)                 
        
        while True:
            global MESSAGE_COUNT,MESSAGE_SWITCH
            #currentDT = datetime.datetime.now()            
            if MESSAGE_SWITCH:
                # send a few messages every minute
                log.info( "IoTHubClient sending %d messages" % MESSAGE_COUNT )
                
                reverseTimestamp = str(datetime.utcnow())
                reverseTimestamp = '{:20f}'.format(MAX_VALUE - (calculateTicks(datetime.utcnow())))
                
                MSG_TXT["PayLoadTimeStamp"] = str(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
                MSG_TXT["ReverseTimeStamp"] = reverseTimestamp
                
                #Barometer Bricklet                
                #
                MSG_TXT["Payload"]["baro_temperature"] = barometer.get_temperature() / 100.0
                MSG_TXT["Payload"]["baro_airpressure"] = barometer.get_air_pressure() / 1000.0
                MSG_TXT["Payload"]["baro_altitude"] = barometer.get_altitude() / 1000.0
                # Air Quality Bricklet
                #
                aq_iaq_index, aq_iaq_accuracy, aq_temperature, aq_humidity, \
                aq_air_pressure = airQuality.get_all_values()
                
                MSG_TXT["Payload"]["aq_iaq_index"] = aq_iaq_index
                MSG_TXT["Payload"]["aq_iaq_accuracy"] = aq_iaq_accuracy
                MSG_TXT["Payload"]["aq_temperature"] = aq_temperature / 100.0
                MSG_TXT["Payload"]["aq_humidity"] = aq_humidity / 100.0
                MSG_TXT["Payload"]["aq_air_pressure"] = aq_air_pressure / 100.0                                          

                # Ambient Light Bricklet 
                #
                MSG_TXT["Payload"]["al_illuminance"] = ambientLight.get_illuminance() / 100.0
                
                #Motion Detection Bricklet
                #
                MSG_TXT["Payload"]["motion_detector"] = motionDetector.get_motion_detected()
                

                
                log.info(str(MSG_TXT))
                
                #print(str(MSG_TXT))
                
                message = IoTHubMessage(str(MSG_TXT))
                
                client.send_event_async(message, send_confirmation_callback, MESSAGE_COUNT)
                log.info( "IoTHubClient.send_event_async accepted message [%d] for transmission to IoT Hub." % MESSAGE_COUNT )

                status = client.get_send_status()
                log.info( "Send status: %s" % status )
                MESSAGE_COUNT += 1
            time.sleep(60.0)            

    except IoTHubError as iothub_error:
        log.error( "Unexpected error %s from IoTHub" % iothub_error )
        return
    except KeyboardInterrupt:
        log.error("IoTHubClient sample stopped")

    print_last_message_time(client)

def parse_iot_hub_name():
    m = re.search("HostName=(.*?)\.", CONNECTION_STRING)
    return m.group(1)

if __name__ == "__main__":   
    ipcon = IPConnection()
    while True:
        try:
            ipcon.connect(HOST, PORT)            
            break
        except Error as e:
            log.error("Connection Error: " + str(e.description))
            time.sleep(1)
        except socket.error as e:
            log.error("Socket error: " + str(e.description))
            time.sleep(1)       
            
    ipcon.register_callback(IPConnection.CALLBACK_ENUMERATE, cb_enumerate)
    ipcon.register_callback(IPConnection.CALLBACK_CONNECTED, cb_connected)
    
    while True:
        try:
            ipcon.enumerate()
            break
        except Error as e:
            log.error('Enumerate Error: ' + str(e.description))
            time.sleep(1)       
            
    iothub_client_run()
