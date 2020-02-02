import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{from_json,col}
object solucionPractica {
  def main(args: Array[String]):Unit={
    // create spark object with two nodes (local[2])
    val spark = SparkSession.builder().appName("appPracticaBDProcessing").master("local[2]").getOrCreate()

    // load a streaming Dataset from external storage systems in this case kafka
    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topicpractica")
      .option("startingOffsets", "earliest")
      .load()

    // cast data from kakfka to String
    val res = ds.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("id", IntegerType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("email", StringType)
      .add("gender", StringType)
      .add("ip_address", StringType)

    // make selection and filtering of data. It must filter out the first two json objects
    val person = res.select(from_json(col("value"), schema).as("data")).select("data.*")
      .filter("data.first_name != 'Giavani'")
      .filter("data.last_name != 'Penddreth'")

    // write a streaming Dataset to external storage systems, in this case the console
    person.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
