package deltalake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Helper {

  def writeData(data: Dataset[java.lang.Long], path: String):Unit = {
    data
      .write
      .mode("overwrite")
      .format("delta")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
      .save(path)
  }

  def UpdateDeltaTable(path: String):Unit ={
    val deltaTable = DeltaTable.forPath(path)

    // Update every even value by adding 100 to it
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100")))
  }

  def DelValue(path: String): Unit ={
    val deltaTable = DeltaTable.forPath(path)
    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))
  }

  def UpsertData(path:String, newData: DataFrame): Unit={
    val deltaTable = DeltaTable.forPath(path)
    // Upsert (merge) new data

    deltaTable.as("oldData")
      .merge(
        newData,
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

  }

  def GetHistory(path: String): Unit={
    val deltaTable = DeltaTable.forPath(path)
    val fullHistoryDF = deltaTable.history()
    print(fullHistoryDF.show(false))
    fullHistoryDF.write.parquet("C:\\Users\\212747507\\IdeaProjects\\DeltaLake\\test")
  }

  def GetVacuum(path: String): Unit={
    val deltaTable = DeltaTable.forPath(path)
    val vacuumHistoryDF = deltaTable.vacuum()
    print(vacuumHistoryDF.show(false))

  }

  def readMySQL(spark: SparkSession): DataFrame = {
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "actor")
      .option("user", "root")
      .option("password", "cloudera").load()
  }

}
