import sys
import configparser
import psycopg2
import logging
import os
import csv

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
	filename='logs/development.log',level=logging.INFO, datefmt='[%Y-%m-%d %H:%M:%S]')
logging.info('analyze_data <<')

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
	postgres_config = config["postgresql"]
except KeyError as e:
	logging.info("Config file missing section \"" + e.args[0] + "\"")
	exit(1)

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
	logging.info("dump_data:config missing key \"" + e.args[0] + "\"")
	exit(1)

cur = db_conn.cursor()

logging.info("producer_id, urls, product_id, product_name, price, created_at")
cur.execute("SELECT count(*), product_id from webanalytics where urls = 'order' group by product_id order by count(*) desc limit 1")
x = cur.fetchone()
if x is None:
	logging.info("No best seller by quantity")
else:
	logging.info("Best seller by quantity: product '{}' sold {} times".format(products[int(x[0])-1][1], x[1]))

cur.execute("SELECT product_id, sum(price) from webanalytics where urls = 'order' group by product_id order by sum(price) desc limit 1")
x = cur.fetchone()
if x is None:
	logging.info("No best seller by revenue")
else:
	logging.info("Best seller by revenue: product '{}' sold total price ${}".format(products[int(x[0])-1][1], x[1]))

cur.execute("SELECT count(*), product_id from webanalytics where urls = 'related_product' group by product_id order by count(*) desc limit 1")
x = cur.fetchone()
if x is None:
	logging.info("No most viewed related products")
else:
	logging.info("Most viewed related products: product '{}' is viewed as related product for {} times".format(products[int(x[0])-1][1], x[1]))


db_conn.close()
