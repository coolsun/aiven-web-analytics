import sys
import os
import configparser
import kafka
import uuid
import logging
import csv
import time
import random

current_milli_time = lambda: int(round(time.time() * 1000))

def hello_world:
	print "Hello World"
	
def send_callback(x):
	pass

def send_errback(x):
	logging.error(x)
	exit(1)

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
	filename='logs/development.log',level=logging.INFO, datefmt='[%Y-%m-%d %H:%M:%S]')
logging.info('producer << ')

with open('data/products.csv', newline='') as csvfile:
	products_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
	products = []
	for row in products_reader:
		if len(row) > 0 and str.isnumeric(row[0]):
			products.append(row)
with open('data/urls.csv', newline='') as csvfile:
	urls_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
	urls = []
	for row in urls_reader:
		if len(row) > 0 and row[0] != 'urls':
			urls.append(row[0])
	logging.info(urls)

config_file = "config.ini"
if not os.path.exists(config_file):
	logging.error("Error:" + config_file + " does not exist!:exit(1)")
	exit(1)

config = configparser.ConfigParser()
config.read(config_file)

try:
	kafka_config = config["kafka"]
	topic = kafka_config["topic"]
	sleep_second = int(kafka_config["sleep"])
	producer_config = config["producer"]
	records = int(producer_config["records"])
except KeyError as e:
	logging.info("Config file missing section or key \"" + e.args[0] + "\"")
	exit(1)

try:
	producer = kafka.KafkaProducer(
		bootstrap_servers = kafka_config["host"] + ":" + kafka_config["port"],
		security_protocol = "SSL",
		ssl_cafile = kafka_config["cafile"],
		ssl_certfile = kafka_config["certfile"],
		ssl_keyfile = kafka_config["keyfile"],
	)
except KeyError as e:
	logging.info("Kafka config missing key \"" + e.args[0] + "\"")
	exit(1)

producer_id = str(uuid.uuid4())
logging.info("producer_id: " + producer_id)
#urls = ["index", "index", "index", "product", "product", "related_product", "cart", "order"]

for i in range(records):
	e_time = str(current_milli_time())
	#key = (producer_id + '|' + str(e_time) + '|' + '{0:04}'.format(i)).encode()
	key = (producer_id + '|' + str(i))
	url = urls[random.randint(0, len(urls)-1)]
	# select related_product id if the destination url is related_product
	if url == "related_product":
		data = (url + '|' + str(products[random.randint(0, len(products)-1)][2]) + '|' + e_time)
	else:
		data = (url + '|' + str(products[random.randint(0, len(products)-1)][0]) + '|' + e_time)
	logging.info("Sending web analytics:i="+str(i)+":key="+str(key)+":data="+str(data)+":start...") 
	# topic as the destination
	(producer.send(topic, data.encode(), key=key.encode())
		.add_callback(send_callback)
		.add_errback(send_errback)
	)
	producer.flush()
	logging.info("Sending web analytics:i="+str(i)+":key="+str(key)+":data="+str(data)+":end...") 
	time.sleep(random.randint(0, sleep_second*100)/100)
time.sleep(3)