import json
class ParquetCommand:
    def execute(self, spark, *args, **kargs):
        op = kargs.get('op', 'read')
        src = kargs.get('src')
        src_format = kargs.get('src_format', 'csv')
        des = kargs.get('des', None)
        row = kargs.get('row', 0)
        
        if op =='read':
            df = spark.read.format("parquet") \
            .load(src)
            print '===================== PRINT PARQUET ====================='
            df.show(row)
        else:
            if src_format=='csv':
                source_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(src)
                print '===================== PRINT CSV ====================='
                source_df.show(row)
            else:
                source_df = spark.read.format("json") \
                .option("inferSchema", "true") \
                .load(src)
                print '===================== PRINT JSON ====================='
                source_df.show(row)
                 
            source_df.write.format("parquet").mode("overwrite").save(des)
            # partitioning
#             source_df.write.format("parquet").mode("overwrite").partitionBy("DEST_COUNTRY_NAME").save(des + '.partition')
            # bucketing
#             source_df.write.format("parquet").mode("overwrite").bucketBy(10, "count").saveAsTable('bucket')
            
            df = spark.read.format("parquet") \
            .load(des)
            print '===================== PRINT PARQUET ====================='
            df.show(row)
        return df