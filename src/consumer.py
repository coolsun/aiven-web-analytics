import sys
import configparser
import kafka
import uuid
import psycopg2
import logging
import csv
import os
import time

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
	filename='logs/development.log',level=logging.INFO, datefmt='[%Y-%m-%d %H:%M:%S]')
logging.info('consumer <<')

with open('data/products.csv', newline='') as csvfile:
	products_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
	products = []
	for row in products_reader:
		if len(row) > 0 and str.isnumeric(row[0]):
			products.append(row)
		logging.info(', '.join(row))
with open('data/urls.csv', newline='') as csvfile:
	urls_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
	urls = []
	for row in urls_reader:
		if len(row) > 0 and row[0] != 'urls':
			urls.append(row[0])
		logging.info(', '.join(row))
	logging.info(urls)

config_file = "config.ini"
if not os.path.exists(config_file):
	logging.error("Error:" + config_file + " does not exist!:exit(1)")
	exit(1)

config = configparser.ConfigParser()
config.read(config_file)

try:
	kafka_config = config["kafka"]
	postgres_config = config["postgresql"]
except KeyError as e:
	logging.error("Config file missing section \"" + e.args[0] + "\"")
	exit(1)

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
	logging.error("Kafka config missing key \"" + e.args[0] + "\"")
	exit(1)

try:
	db_conn = psycopg2.connect(
		host = postgres_config["host"],
		port = postgres_config["port"],
		dbname = postgres_config["dbname"],
		user = postgres_config["user"],
		password = postgres_config["password"],
		sslmode = "require"
	)
except KeyError as e:
	logging.error("PostgreSQL config missing key \"" + e.args[0] + "\"")
	exit(1)

db_conn.set_session(autocommit=False)
cur = db_conn.cursor()
try:
	cur.execute(
	"""CREATE TABLE IF NOT EXISTS webanalytics (
		id SERIAL PRIMARY KEY,
		producer_id varchar(255) NOT NULL,
		urls varchar(100) NOT NULL,
		product_id integer NOT NULL,
		product_name varchar(100) NOT NULL,
		price float NOT NULL,
		created_at timestamp NOT NULL
	);"""
	)
except e:
	logging.info(e)

def insert_record(producer_id, urls, product_id, product_name, price, created_at):
	#logging.info(producer_id, urls, product_id, product_name, price, created_at)
	try:
		cur.execute(
		"""INSERT INTO webanalytics
			(producer_id, urls, product_id, product_name, price, created_at) VALUES
			(%s, %s, %s, %s, %s, %s)
		""", (producer_id, urls, product_id, product_name, price, created_at))
	except psycopg2.IntegrityError as e:
		logging.info("Warning: click already exists in database")
		db_conn.rollback()


for m in consumer:
	try:
		key = m.key.decode()
		logging.info("consumer:key={}".format(key))
		keys = key.split('|')
		producer_id = keys[0]
		e_time = keys[1]
	except Exception as e:
		logging.info("Error: get Key", e)
		continue

	data = m.value.decode()
	data_arr = data.split('|')
	url = data_arr[0]
	product_id = int(data_arr[1])
	created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(data_arr[2])/1000))
	product_name = products[product_id-1][1]
	price = products[product_id-1][3]
	insert_record(producer_id, url, product_id, product_name, price, created_at)
	db_conn.commit()
	# commit m in kafka
	m_cur = {}
	tp = kafka.TopicPartition(m.topic, m.partition)
	m_cur[tp] = kafka.OffsetAndMetadata(m.offset+1, None)
	consumer.commit(m_cur)
	logging.info("consumer >> key={}".format(key))
