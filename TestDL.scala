package deltalake

import org.apache.spark.sql.SparkSession
import deltalake.Helper._

object TestDL {

  System.setProperty("hadoop.home.dir", "C:\\subhrajit\\winutils")
  private val spark = getAndConfigureSparkSession()

  def getAndConfigureSparkSession() = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("DataLake test")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {

    val path = "C:\\Users\\212747507\\IdeaProjects\\DeltaLake\\tmp\\delta-table"

    val data = spark.range(0, 5)
//    print("Temporary Data" + data.show())
    writeData(data,path)

//    val df = spark.read.format("delta").load(path)
//    print(df.show())

    /*Update*/
    UpdateDeltaTable(path)
//    val up_df = spark.read.format("delta").load(path)
//    print("Update Data" + up_df.show())

    /*Delete*/
    DelValue(path)
//    val ddf = spark.read.format("delta").load(path)
//    print("Delete Data" + ddf.show())

    /*Upsert*/
    val newData = spark.range(0, 20).as("newData").toDF
    UpsertData(path, newData)

//    val udf = spark.read.format("delta").load(path)
//    print("Merge Data" + udf.show(1000))

    GetHistory(path)

//    GetVacuum(path)

    spark.stop()

  }


}
