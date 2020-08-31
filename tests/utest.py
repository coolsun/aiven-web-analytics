# tests/utest.py
import unittest
import pytest
import os
import sys
import configparser
import kafka
import logging
import uuid
import psycopg2

class TestWebAnalyticsMethods(unittest.TestCase):

    def setUp(self):
        print('setUp')


    def tearDown(self):
        print('tearDown')

    @pytest.fixture
    def input_str():
        input = 'webanalytics'
        return input

    # check if the config file config.ini exists
    def test_if_exists_config_file(self):
        print("test_if_exists_config_file")
        self.assertEqual(True, os.path.exists("./config.ini"))

    def test_create_producer(self):
        print("test_create_producer")
        is_exception = False
        config_file = "config.ini"
        config = configparser.ConfigParser()
        config.read(config_file)
        try:
            kafka_config = config["kafka"]
            topic = kafka_config["topic"]
            sleep_second = int(kafka_config["sleep"])
            producer_config = config["producer"]
            records = int(producer_config["records"])
        except KeyError as e:
            print("Config file missing section or key \"" + e.args[0] + "\"")
            is_exception = True
        try:
            producer = kafka.KafkaProducer(
            bootstrap_servers = kafka_config["host"] + ":" + kafka_config["port"],
            security_protocol = "SSL",
            ssl_cafile = kafka_config["cafile"],
            ssl_certfile = kafka_config["certfile"],
            ssl_keyfile = kafka_config["keyfile"],
	    )
        except KeyError as e:
            print("Kafka config missing key \"" + e.args[0] + "\"")
            is_exception = True
        self.assertEqual(False, is_exception)

    def test_create_consumer(self):
        print("test_create_consumer")
        is_exception = False
        config_file = "config.ini"
        config = configparser.ConfigParser()
        config.read(config_file)
        try:
            kafka_config = config["kafka"]
            postgres_config = config["postgresql"]
        except KeyError as e:
            print("Config file missing section \"" + e.args[0] + "\"")
            is_exception = True
        try:
            consumer = kafka.KafkaConsumer(
                kafka_config["topic"],
                client_id = "web-analytics-client-id-"+str(uuid.uuid4()),
                group_id = kafka_config["topic"] + "_consumers",
                bootstrap_servers = kafka_config["host"] + ":" + kafka_config["port"],
                security_protocol = "SSL",
                ssl_cafile = kafka_config["cafile"],
                ssl_certfile = kafka_config["certfile"],
                ssl_keyfile = kafka_config["keyfile"],
                # continue reading from the earliest non-committed message
                auto_offset_reset = "earliest",
                # commit only after the results have been pushed to the database
                enable_auto_commit = False,
            )
        except KeyError as e:
            print("Kafka config missing key \"" + e.args[0] + "\"")
            is_exception = True
        self.assertEqual(False, is_exception)

    def test_db_connection(self):
        print("test_db_connection")
        is_exception = False
        config_file = "config.ini"
        config = configparser.ConfigParser()
        config.read(config_file)

        try:
            postgres_config = config["postgresql"]
        except KeyError as e:
            print("Config file missing section \"" + e.args[0] + "\"")
            is_exception = True

        try:
            db_conn = psycopg2.connect(
                host = postgres_config["host"],
                port = postgres_config["port"],
                dbname = postgres_config["dbname"],
                user = postgres_config["user"],
                password = postgres_config["password"],
                sslmode = "require",
            )
        except KeyError as e:
            print("dump_data:config missing key \"" + e.args[0] + "\"")
            is_exception = True
        self.assertEqual(False, is_exception)
if __name__ == '__main__':
    unittest.main()
