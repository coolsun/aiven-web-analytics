# aiven-web-analytics-kafka-python-postgreSQL

Demonstrates Ecommerce web analytics collection with Aiven's Kafka 
and PostgresQL services.

Simulation data with two csv files:
1. products.csv: a list of products, their related products, and price
2. urls.csv: a list of virtual urls such as index, search, product, related_product, cart, and order.  Multiple entries for each url are allowed.  The producer randomly hits the urls such that the more entries of a url, the more chance it will be hit.

Prerequisites: 
1. Aiven Kafka service or other's
2. Aiven PostgreSQL service or other's
3. Kafka topic(set in config.ini) should be created first
4. set up ca.pem, service.cert, service.key under /ssl folder to allow access
   to Kafka and PostgreSQL

Install dependencies:
```
pip3 install -r requirements.txt
```

Modify the configuration file:
```
./config.ini
${EDITOR} ./config.ini
```

Run producer:
```
python3 src/producer.py
```
Note: 
1. number of records sent each time can be changed in config.ini > Kafka > records
2. producer randomly sleep between sending requests.  Max sleep time is in config.ini > Producer > Sleep (second)

Run consumer:
```
python3 src/consumer.py
```
Note:
1. data will be write to the table (default as webanalytics) set in config.ini > consumer > table.

Query the webanalysis table to retrieve web analytics or metrics such as best seller by total price or by volume as well as the most viewed related products:
```
python3 src/analyze_data.py
```
Note: read from the table (default as webanalytics) set in config.ini > consumer > table.
Run tests: 
```
python3 tests/utest.py
```
Four tests will be run as follows:
1. test_create_consumer
2. test_create_producer
3. test_db_connection
4. test_if_exists_config_file

Result:
python3 src/analyze_data.py can return many valuable web analytics and allows the 
system to fine tune or even dynamically adjust the content or product delivered to
end user.
1. Best seller product by volume
2. Best seller product by total price
3. Most viewed related product


Credit and thanks to the following authors and sample code:
1. Using Kafka for web application metrics:
    https://towardsdatascience.com/using-kafka-for-collecting-web-application-metrics-in-your-cloud-data-lake-b97004b2ce31
2. Getting started with Aiven Kafka
    https://aiven.io/blog/getting-started-with-aiven-kafka
3. Kafka Streams Examples
    https://github.com/confluentinc/kafka-streams-examples
4. Kafka-Python explained in 10 lines of code
    https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
5. Aiven Sample code
    https://github.com/viinikv


