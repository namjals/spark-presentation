from pyspark.sql import SparkSession
import pyspark
from flask import request
from flask import Flask
import json
import os
from decimal import Decimal
app = Flask('miniproject')
external_command_path = os.path.dirname(os.path.abspath(__file__))

@app.route('/command', methods=['POST'])
def command():
    body = json.loads(request.data)
    command = importer(body['filename'])
    result = command.execute(spark, **body)
    if isinstance(result, pyspark.sql.dataframe.DataFrame):
        result = result.take(int(body['row']))
    return json.dumps({'result': result}, default=encoder)
    
def importer(name):
    import imp
    import inspect
    def _external_module_predicate(member):
        return inspect.isclass(member) and member.__module__ == name
    py_path = "%s/%s" % (external_command_path, name)
    print 'py_path : ' + py_path
    cmd_module = imp.load_source(name, py_path)
    cls_list = inspect.getmembers(cmd_module, _external_module_predicate)
    for _, cls in cls_list:
        return cls()

def encoder(data):
    if isinstance(data, Decimal):
        return str(data)  
    
def spark_init():
    global spark
    spark = SparkSession.builder \
        .master("local") \
        .appName("miniproject") \
        .config("spark.sql.shuffle.partitions","3") \
        .config("spark.jars","lib/sqlite-jdbc-3.23.1.jar") \
        .getOrCreate()
#     spark.conf.set("spark.sql.shuffle.partitions", "5")

def start():
    spark_init()
    app.run(host='0.0.0.0', port=5000, threaded=True)
    

if __name__ == "__main__":
    start()
