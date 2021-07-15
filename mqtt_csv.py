import paho.mqtt.client as mqtt
import time
import os
import glob
import pandas as pd
import time


class mqtt_connect:
    def __init__(self):
        self.disconnect_flag = False
        self.client = ""

    def on_connect(self, client, userdata, flag, rc):
        self.disconnect_flag = False
        print("Successfully connected to server")

    def on_disconnect(self, client, userdata, flag):
        self.disconnect_flag = True
        print("Unable to connect to the server. Please check the internet setting.")
        quit()

    def on_publish(self, client, userdata, mid):
        print("publish: {0}".format(mid))

    def make_mqtt_session(self):
        client = mqtt.Client()
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        self.client = client

    def publish_mqtt(self, data_header, data_cdc, data_imp):
        self.client.on_publish = self.on_publish  # callback of sending message
        self.client.connect("Mqtt.synerex.net", 1883, 60)

        # Start the process
        self.client.loop_start()

        if self.disconnect_flag is False:
            # last_update_date = mf.get_last_update_date()
            # self.client.publish("office/fukuoka/3f/status", "updated")
            # Sending message to the specific Topic
            self.client.publish("office/fukuoka/3f/header", data_header)
            self.client.publish("office/fukuoka/3f/CD-curve", data_cdc)
            self.client.publish("office/fukuoka/3f/imp", data_imp)

        else:  # Shut down the program due to the unsuccessful connection with the MQTT server
            print("[ERROR] Unexpected disconnected. Program will be shut down in 2 seconds.")
            time.sleep(2)
            quit()


class mqtt_upload_file:
    def __init__(self, directory):
        self.directory = directory
        self.data_header, self.filename_header = self.get_csv_file_contents(0)
        self.parameter_cdc = self.data_header[3][1]
        self.parameter_imp = self.data_header[4][1]
        if self.parameter_cdc == "1":
            self.data_cdc, self.filename_cdc = self.get_csv_file_contents(1)
            self.data_cdc = self.data_cdc.to_csv(header=False, index=False)

        else:
            self.data_cdc = "null"
            self.filename_cdc = "null"

        if self.parameter_imp == "1":
            self.data_imp, self.filename_imp = self.get_csv_file_contents(2)
            self.data_imp = self.data_imp.to_csv(header=False, index=False)

        else:
            self.data_imp = "null"
            self.filename_imp = "null"

        self.data_header = self.data_header.to_csv(header=False, index=False)

    def get_latest_file(self, file_type):
        list_of_files = []
        if file_type == 0:
            list_of_files = glob.glob(self.directory + "/header_*.csv")

        elif file_type == 1:
            list_of_files = glob.glob(self.directory + "/cdc_*.csv")

        elif file_type == 2:
            list_of_files = glob.glob(self.directory + "/imp_*.csv")
            # print(list_of_files)

        else:
            print("[ERROR] Please check the class parameters or the csv files sheets name is correct. [get_latest_file()]")
            quit()

        if list_of_files:
            # print(list_of_files)
            latest_file_path = max(list_of_files, key=os.path.getctime)
            # print(latest_file_path)
            latest_file_name = os.path.basename(latest_file_path)[:-4]  # remove ".csv"
            return latest_file_path, latest_file_name

        else:
            print("[ERROR] There is no any csv file in the folder. [get_latest_file()]")
            quit()

    def get_csv_file_contents(self, file_type):
        latest_file_path, latest_file_name = self.get_latest_file(file_type)
        f = pd.read_csv(latest_file_path, header=None)
        f = f.drop(columns=[0], axis=1)
        return f, latest_file_name


if __name__ == "__main__":
    mqtt_connection = mqtt_connect()
    work_directory = "./data"
    # while True:  # Delete "while True" if you want to run the program only one time
    a = mqtt_upload_file(work_directory)
    mqtt_connection.make_mqtt_session()
    mqtt_connection.publish_mqtt(a.data_header, a.data_cdc, a.data_imp)
    time.sleep(5)  # wait for update complete

    # try:
    #     while True:     # Delete "while True" if you want to run the program only one time
    #         a = mqtt_upload_file(work_directory)
    #         mqtt_connection.make_mqtt_session()
    #         mqtt_connection.publish_mqtt(a.data_header, a.data_cdc, a.data_imp)
    #         sleep(5)    # update the data every 5 seconds (for demo)
    #
    # except Exception as e:
    #     print("[ERROR] Please check the internet setting.")
    #     print(e)

