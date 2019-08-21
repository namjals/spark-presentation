from flask import request
from flask import Flask
import json
app = Flask('miniproject')

@app.route('/command', methods=['POST'])
def command():
    body = json.loads(request.data)
    command = importer(body['filename'])
    result = command.execute(spark)
    return json.dumps({"body":body, 'result': result})
    
# @app.route('/spark_job', methods=['GET'])
# def spark_job():
#     df1 = spark.range(2, 10000000, 2)
#     df2 = spark.range(2, 10000000, 4)
#     step1 = df1.repartition(5)
#     step12 = df2.repartition(6)
#     step2 = step1.selectExpr("id * 5 as id")
#     step3 = step2.join(step12, ["id"])  
#     step4 = step3.selectExpr("sum(id)")
#     ret = step4.collect()
#     return json.dumps({"a":ret})

def importer(name):
    import imp
    import inspect
    def _external_module_predicate(member):
        return inspect.isclass(member) and member.__module__ == name
    external_command_path = '/home/demo/test/miniproject'
    py_path = "%s/%s" % (external_command_path, name)
    cmd_module = imp.load_source(name, py_path)
    cls_list = inspect.getmembers(cmd_module, _external_module_predicate)
    for _, cls in cls_list:
        return cls()

def spark_init():
    from pyspark.sql import SparkSession
    global spark
    spark = SparkSession.builder \
        .master("spark://10.10.10.100:7077") \
        .appName("miniproject") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

def start():
    spark_init()
    app.run(host='0.0.0.0', port=5000, threaded=True)
    

if __name__ == "__main__":
    start()
