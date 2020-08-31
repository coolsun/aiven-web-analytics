# aiven-web-analytics

Demonstrates Ecommerce web analytics collection and analysis via real-time streaming service with Aiven's powerful and ease-of-use Kafka and PostgresQL services.

Simulation data with two csv files:
1. products.csv: a list of products, their related products, and price
2. urls.csv: a list of virtual urls such as index, search, product, related_product, cart, and order.  Multiple entries for each url are allowed.  The producer randomly hits the urls such that the more entries of a url, the more chance it will be hit.

Prerequisites: Set up before running the programs
1. Aiven Kafka service or equivalent
2. Aiven PostgreSQL service or eqvivalent
3. Kafka topic(set in config.ini) should be created first
4. set up ca.pem, service.cert, service.key under /ssl folder to allow access
   to Kafka and PostgreSQL

Install dependencies:
```
pip3 install -r requirements.txt
```

Set up and modify the configuration file:
```
./config.ini
${EDITOR} ./config.ini
```

Run producer: Output to logs/development.log
```
python3 src/producer.py
```
Note: 
1. number of records sent each time can be changed in config.ini > kafka > records
2. producer randomly sleep between sending requests.  Max sleep time is in config.ini > producer > sleep (second)

Run consumer: Output to logs/development.log
```
python3 src/consumer.py
```
Note:
1. data will be written to the table (default webanalytics) set in config.ini > consumer > table.

Run analysis: Output to logs/development.log
```
python3 src/analyze_data.py
```
* read from the table (default as webanalytics) set in config.ini > consumer > table.
* Query the webanalysis table to retrieve web analytics or metrics such as best seller by revenue or by volume as well as the most viewed related products.
* python3 src/analyze_data.py can return many valuable web analytics and allows the 
system to fine tune or even dynamically adjust the product or content delivered to
end user.

Currently there are three queies for the results.
1. Best seller product by volume
2. Best seller product by revenue
3. Most viewed related product

Here is a sample output in logs/development.log
* [2020-08-31 09:03:41] INFO   Best seller by quantity: product 'mug2' sold 14 times
* [2020-08-31 09:03:56] INFO   Best seller by revenue: product 'tshirt1' sold total price $80.0
* [2020-08-31 09:04:03] INFO   Most viewed related products: product 'mug6' is viewed as related product for 12 times

Run tests: 
```
python3 tests/utest.py
```
Four tests will be run as follows:
1. test_create_consumer
2. test_create_producer
3. test_db_connection
4. test_if_exists_config_file

Sample output of the four tests:
```
setUp
test_create_consumer
tearDown
.setUp
test_create_producer
tearDown
.setUp
test_db_connection
tearDown
.setUp
test_if_exists_config_file
tearDown
.
----------------------------------------------------------------------
Ran 4 tests in 0.871s

OK
```

Credit and thanks to the following authors and sample code:
1. Getting started with Aiven Kafka
    https://aiven.io/blog/getting-started-with-aiven-kafka
2. Kafka Streams Examples
    https://github.com/confluentinc/kafka-streams-examples
3. Kafka-Python explained in 10 lines of code
    https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
4. Aiven Sample code
    https://github.com/viinikv


