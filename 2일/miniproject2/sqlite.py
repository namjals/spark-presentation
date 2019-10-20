import json
class SqliteCommand:
    def execute(self, spark, *args, **kargs):
        op = kargs.get('op', 'read')
        src = kargs.get('src')
        tablename = kargs.get('tablename')
        row = kargs.get('row', 0)

        if op =='read':
            df = spark.read.format("jdbc") \
            .option("url", 'jdbc:sqlite:'+ src) \
            .option("dbtable", tablename) \
            .option("driver", "org.sqlite.JDBC") \
            .load()
            print '===================== PRINT SQLITE ====================='
            df.show(row)
        else:
            src_format = kargs.get('src_format', 'csv')
            des = kargs.get('des')
            if src_format=='csv':
                source_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(src)
                print '===================== PRINT CSV ====================='
                source_df.show(row)
            source_df.write.jdbc('jdbc:sqlite:' + des, tablename, mode="overwrite", properties={"driver": "org.sqlite.JDBC"})
#             source_df.write.jdbc('jdbc:sqlite:' + des, tablename, mode="append", properties={"driver": "org.sqlite.JDBC"})
            df = spark.read.format("jdbc") \
            .option("url", 'jdbc:sqlite:'+ des) \
            .option("dbtable", tablename) \
            .option("driver", "org.sqlite.JDBC") \
            .load()
            print '===================== PRINT SQLITE ====================='
            df.show(row)
            print df.count()

        return df