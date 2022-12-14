install:
	@pip install -r requirements.txt

pyspark:
	@pyspark \
    --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
    --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"

start:
	@jupyter-lab

clean:
	@rm -rf out/*tbl
	@rm -rf out/*table
