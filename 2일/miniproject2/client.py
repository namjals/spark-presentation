import json
from httplib import HTTPConnection
import sys

def http_request(url, method="GET", host="localhost", port=8767, **body):
    http_conn = HTTPConnection(host, port)
    http_conn.request(method, url, json.dumps(body))
    results = json.load(http_conn.getresponse())
    http_conn.close()
    return results

# print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='command.py')

# # Read DataSource
print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='parquet.py', op='read', row=10, src='data/2010-summary.parquet')
# print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='parquet.py', op='read', row=10, src='data/write.parquet.partition')
# print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='sqlite.py', op='read', row=10, src='data/my-sqlite.db', tablename='flight_info')


# # Write DataSource
# print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='parquet.py', op='write', src='data/read.json', des='data/write.parquet', src_format='json', row=10)
# print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='parquet.py', op='write', src='data/read.csv', des='data/write.parquet', src_format='csv', row=10)
# print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='sqlite.py', op='write', src='data/read.csv', des='data/write.db', src_format='csv', tablename='flight_info', row=10)
