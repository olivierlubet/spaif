package net.lubet.spaif

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Context {
  val mock = false

  lazy val spark: SparkSession = {
    System.setSecurityManager(null)

    //val warehouseLocation = "file:///Users/olivi/IdeaProjects/funimmocrawl/spark"
    val dataLocation= new File("../data").
      getAbsolutePath.replace("C:\\", "/") // pour Windows

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println(dataLocation+"/metastore_db")
    SparkSession
      .builder
      .master("local[4]")
      .appName("FIC")
      .config("spark.sql.warehouse.dir", s"file://$dataLocation/warehouse")
      .config("javax.jdo.option.ConnectionURL",s"jdbc:derby:;databaseName=$dataLocation/metastore_db;create=true")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.ui.port","8081")
      .enableHiveSupport()
      .getOrCreate()
  }
}