import json
from httplib import HTTPConnection
import sys

def http_request(path, method="GET", host="localhost", port=8767, **body):
    http_conn = HTTPConnection(host, port)
    http_conn.request(method, path, json.dumps(body))
    results = json.load(http_conn.getresponse())
    http_conn.close()
    return results

# print http_request("/gaia/repository", method="GET", host='0.0.0.0', port='5000')
# print http_request("/spark_job", method="GET", host='0.0.0.0', port='5000')
print http_request("/command", method="POST", host='0.0.0.0', port='5000', filename='command.py')