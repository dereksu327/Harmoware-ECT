import paho.mqtt.client as mqtt
from time import sleep
import os
import glob
import pandas as pd
import time
import json
import itertools
import csv


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

    def publish_mqtt(self, data):
        self.client.on_publish = self.on_publish  # callback of sending message
        self.client.connect("Mqtt.synerex.net", 1883, 60)

        # Start the process
        self.client.loop_start()

        if self.disconnect_flag is False:
            # last_update_date = mf.get_last_update_date()
            # self.client.publish("office/fukuoka/3f/status", "updated")
            # Sending message to the specific Topic
            self.client.publish("office/fukuoka/3f", data)

        else:  # Shut down the program due to the unsuccessful connection with the MQTT server
            print("[ERROR] Unexpected disconnected. Program will be shut down in 2 seconds.")
            time.sleep(2)
            quit()


class mqtt_upload_file:
    def __init__(self, directory):
        self.directory = directory
        # self.data_header, self.filename_header = self.get_csv_file_contents(0)
        self.parameter_cdc, self.parameter_imp = self.get_upload_parameters()
        self.data = self.csv_data_to_json(self.get_latest_file(0), self.get_latest_file(1), self.get_latest_file(2))

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
            latest_file_path = max(list_of_files, key=os.path.getctime)
            # latest_file_name = os.path.basename(latest_file_path)[:-4]  # remove ".csv"
            return latest_file_path

        else:
            print("[ERROR] There is no any csv file in the folder. [get_latest_file()]")
            quit()

    def get_csv_file_contents(self, file_type):
        latest_file_path = self.get_latest_file(file_type)
        f = pd.read_csv(latest_file_path, header=None)
        return f

    def get_upload_parameters(self):
        header_contents = self.get_csv_file_contents(0)
        parameter_cdc = header_contents[3][1]
        parameter_imp = header_contents[4][1]
        return parameter_cdc, parameter_imp

    def csv_data_to_json(self, header_csv_filepath, cdc_csv_filepath, imp_csv_filepath):
        data = {}
        # write header info in JSON
        data['header'] = header_csv_to_json(header_csv_filepath, 0, True)

        # write cdc info in JSON
        if self.parameter_cdc == "1":
            data['cdc_header'] = header_csv_to_json(header_csv_filepath, 2, True)
        else:
            data['cdc_header'] = "null"

        # write imp info in JSON
        if self.parameter_imp == "1":
            data['imp_header'] = header_csv_to_json(header_csv_filepath, 4, True)
        else:
            data['imp_header'] = "null"

        # write cdc info in JSON
        if self.parameter_cdc == "1":
            data['cdc_data'] = header_csv_to_json(cdc_csv_filepath, 0, False)
        else:
            data['cdc_data'] = "null"

        # write imp info in JSON
        if self.parameter_imp == "1":
            data['imp_data'] = header_csv_to_json(imp_csv_filepath, 0, False)
        else:
            data['imp_data'] = "null"

        data = json.dumps(data, indent=4)
        # print(data)
        write_json_file(data)
        return data


def write_json_file(data):
    with open("upload_contents.json", 'w', encoding='utf-8') as jsonf:
        # jsonString = json.dumps(data, indent=4)  # indent is for print layout
        jsonf.write(data)


def header_csv_to_json(csv_filepath, N, isHeader):         # N is the number of rows to skip
    with open(csv_filepath, 'r', encoding='utf-8-sig') as csvf:
        if isHeader is True:
            array = {}
            for row in csv.DictReader(itertools.islice(csvf, N, None)):
                array.update(row)
                break

        else:
            array = []
            for row in csv.DictReader(itertools.islice(csvf, N, None)):
                array.append(row)

        return array


if __name__ == "__main__":
    mqtt_connection = mqtt_connect()
    work_directory = "./data"
    a = mqtt_upload_file(work_directory)
    a.get_upload_parameters()

    try:
        # while True:     # Delete "while True" if you want to run the program only one time
        a = mqtt_upload_file(work_directory)
        mqtt_connection.make_mqtt_session()
        mqtt_connection.publish_mqtt(a.data)
        sleep(5)    # update the data every 5 seconds (for demo)

    except Exception as e:
        print("[ERROR] Please check the internet setting.")
        print(e)

