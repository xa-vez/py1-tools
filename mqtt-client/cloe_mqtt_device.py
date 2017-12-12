import datetime
import json
import time

import jwt
import paho.mqtt.client as mqtt

import requests

project_id="cloe-cloud"
ca_certs="./roots.pem"
private_key_file="./rsa_private.pem"
cloud_region="europe-west1"
http_server="http://cloe-cloud.appspot.com"
algorithm="RS256"
num_messages=10
tracker_IMEIs = ["12312312312"]
application_period_s = 10

def create_jwt(project_id, private_key_file, algorithm):
    """Create a JWT (https://jwt.io) to establish an MQTT connection."""
    token = {
            'iat': datetime.datetime.utcnow(),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            'aud': project_id
    }
    with open(private_key_file, 'r') as f:
        private_key = f.read()
    print 'Creating JWT using {} from private key file {}'.format(
            algorithm, private_key_file)
    return jwt.encode(token, private_key, algorithm=algorithm)


def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))


class Tracker(object):
    #Aux method to activate tracker on 3T server. Needed before we can send information over.
    def http_tracker_activate(self, IMEI):
        activation_data={
              "data": {
              "type" : "trackers",
              "id" : IMEI,
              "attributes" : {
                "activated" : "True",
                "remote" : "True"
                }
              }
            }
        r = requests.patch("{}/api/trackers/{}".format(http_server, IMEI), data=json.dumps(activation_data))
        if r.status_code == 201:
            self.registry_id = "fleet-{}".format(r.json()["data"]["attributes"]["fleet"])
            self.device_id = "tracker-{}".format(IMEI)
        else:
            raise Exception("Activation of tracker with IMEI:%s failed" % IMEI)

    def __init__(self, IMEI):
        self.connected = False

        self.http_tracker_activate(IMEI)

        # Create MQTT client and connect to Cloud IoT.
        self.client = mqtt.Client(
                client_id='projects/{}/locations/{}/registries/{}/devices/{}'.format(
                        project_id, cloud_region, self.registry_id, self.device_id))
        
        self.client.username_pw_set(
                username='unused',
                password=create_jwt(project_id, private_key_file,
                                                        algorithm))
        self.client.tls_set(ca_certs=ca_certs)

        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message

        self.connect()

        # This is the topic that the device will publish telemetry events to.
        mqtt_telemetry_topic = '/devices/{}/events'.format(self.device_id)

        # Update and publish telemetry at a rate of application_period_s per second.
        for _ in range(num_messages):

            # Report the device's position to the server, by serializing it as a JSON
            # string.
            payload = self.generate_payload()
            self.print_debug('Publishing message #%s: ' % _, payload)
            if not self.connected:
                self.connect()
            else:
                self.client.publish(mqtt_telemetry_topic, payload, qos=0)
            
            self.sleep(application_period_s)

        self.client.disconnect()
        self.client.loop_stop()
        self.print_debug('Finished loop successfully. Goodbye!')

    def generate_payload(self):

        #Payload must follow this pattern (if only position is sent, no need to use a list)
        payload = [
            {
                "type" : "positions",
                "attributes" : {
                    "geo_point" : "12.86578817,2.36269871",
                    "created_on" : "2017-12-12T19:47:35.000000"
                    }
            },
            {
                "type" : "positions",
                "attributes" : {
                    "geo_point" : "12.86578817,2.36269871",
                    "created_on" : "2017-12-12T19:48:35.000000"
                    }
            }    
        ]
        return json.dumps(payload)

    # Simulates IoT device sleeping between tranmissions.
    # We stop loop to prevent MQTT client to do operations in background
    # No need to handle reconection since loop_start takes care automatically
    def sleep(self, millisconds):
        self.client.loop_stop()
        time.sleep(application_period_s)
        self.client.loop_start()

    def connect(self):
        self.client.connect("mqtt.googleapis.com", 8883, keepalive=application_period_s*2)
        self.client.loop_start()
        # This is the topic that the device will receive configuration updates on.
        mqtt_config_topic = '/devices/{}/config'.format(self.device_id)

        # Wait up to 5 seconds for the device to connect.
        self.wait_for_connection(5)

        # Subscribe to the config topic.
        self.client.subscribe(mqtt_config_topic, qos=0)


    def wait_for_connection(self, timeout):
        """Wait for the device to become connected."""
        total_time = 0
        while not self.connected and total_time < timeout:
            time.sleep(1)
            total_time += 1

        if not self.connected:
            raise RuntimeError('Could not connect to MQTT bridge.')

    def on_connect(self, unused_client, unused_userdata, unused_flags, rc):
        """Callback for when a device connects."""
        self.print_debug('Connection Result:', error_str(rc))
        self.connected = True

    def on_disconnect(self, unused_client, unused_userdata, rc):
        """Callback for when a device disconnects."""
        self.print_debug('Disconnected:', error_str(rc))
        self.connected = False

    def on_publish(self, unused_client, unused_userdata, unused_mid):
        """Callback when the device receives a PUBACK from the MQTT bridge."""
        self.print_debug('Published message acked.')

    def on_subscribe(self, unused_client, unused_userdata, unused_mid,
                                     granted_qos):
        """Callback when the device receives a SUBACK from the MQTT bridge."""
        self.print_debug('Subscribed: ', granted_qos)
        if granted_qos[0] == 128:
            self.print_debug('Subscription failed.')

    def on_message(self, unused_client, unused_userdata, message):
        """Callback when the device receives a message on a subscription."""
        payload = str(message.payload)
        self.print_debug("Received message '{}' on topic '{}' with Qos {} and client {}".format(
                payload, message.topic, str(message.qos), str(unused_client)))

        # The device will receive its latest config when it subscribes to the config
        # topic. If there is no configuration for the device, the device will
        # receive an config with an empty payload.
        if not payload:
            return

        # The config is passed in the payload of the message. In this example, the
        # server sends a serialized JSON string.
        data = json.loads(payload)
        

    def print_debug(self, *args, **kwargs):
        print "Tracker %s -" % self.device_id , args

def main():
    trackers = []
    for imei in tracker_IMEIs:
        trackers.append(Tracker(imei))
        

if __name__ == '__main__':
    main()



