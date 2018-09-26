package net.lubet.spaif

import scala.io._
import java.io
import java.io.{BufferedWriter, File, FileWriter}
import java.net.{URL, URLEncoder}
import java.text.SimpleDateFormat
import java.sql.{Date, Timestamp}

import org.jsoup.nodes.{Document, Element}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import Context.spark.implicits._
import org.apache.spark.sql.expressions.Window
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

object Euronext {
  def dlList(): String = {
    val url = new URL("https://www.euronext.com/en/popup/data/download?ml=nyx_pd_stocks&cmd=default&formKey=nyx_pd_filter_values%3A1006ef55d4998cc0fad71db6a6f38530")
    val data = Map("format" -> "2",
      "layout" -> "2",
      "decimal_separator" -> "1",
      "date_format" -> "1",
      "op" -> "Go",
      "form_id" -> "nyx_download_form")
    Browser.post(url, data)
  }

  def refreshList = {
    val file = new File("./EuronextList.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(dlList)
    bw.close()
  }

  def getList(refresh: Boolean = false): DataFrame = {
    refreshList

    val schema = StructType(Array(
      StructField("Name", StringType, false),
      StructField("ISIN", StringType, false),
      StructField("Symbol", StringType, false),
      StructField("Market", StringType, false),
      StructField("Trading Currency", StringType, true),
      StructField("Open", DoubleType, true),
      StructField("High", DoubleType, true),
      StructField("Low", DoubleType, true),
      StructField("Last", DoubleType, true),
      StructField("Last Date/Time", TimestampType, true),
      StructField("Time Zone", StringType, true),
      StructField("Volume", DoubleType, true),
      StructField("Turnover", DoubleType, true)
    ))

    val ret = Context.spark.read.
      option("header", value = false).
      option("sep", ";").
      option("timestampFormat", "dd/MM/yyyy HH:mm").
      schema(schema).
      csv("./EuronextList.csv").
      withColumnRenamed("Trading Currency", "Trading_Currency").
      withColumnRenamed("Last Date/Time", "Last_DateTime").
      withColumnRenamed("Time Zone", "Time_Zone").
      repartition($"ISIN")

    ret.write.mode("overwrite").saveAsTable("stock")

    ret
  }

  def dlStock(isin: String, lastDate: Option[Date] = None, targetDate: Option[Date] = None): String = {
    val name: String = "No need"
    val from = lastDate match {
      case None => "946681200000l" // 2000-01-01
      case Some(d) => d.getTime // 2018-09-03 format 1535328000000
    }
    val to = targetDate match {
      case None => (new java.sql.Date(System.currentTimeMillis).getTime + 86400000).toString // 2018-01-01
      case Some(d) => d.getTime // 2018-09-03 format 1535328000000
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //https://www.euronext.com/nyx_eu_listings/price_chart/download_historical?typefile=csv&layout=vertical&typedate=dmy&separator=point&mic=XPAR&isin=FR0010478248&name=ATARI&namefile=Price_Data_Historical&from=1535328000000&to=1535932800000&adjusted=1&base=0
    val url = new URL(s"https://www.euronext.com/nyx_eu_listings/price_chart/download_historical?typefile=csv&layout=vertical&typedate=dmy&separator=point&mic=XPAR&isin=${isin}&name=${URLEncoder.encode(name, "UTF-8")}&namefile=Price_Data_Historical&from=${from}&to=${to}&adjusted=1&base=0")
    println(url.toString)
    Browser.get(url)
  }

  def refreshStock(isin: String, lastDate: Option[Date] = None, targetDate: Option[Date] = None) = {
    val dir = new File("spark/s")
    if (!dir.isDirectory) dir.mkdirs()

    val file = new File(s"spark/s/${isin}.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(dlStock(isin, lastDate, targetDate))
    bw.close()
  }

  def getStock(isin: String): DataFrame = {
    refreshStock(isin)

    val ds = Context.spark.createDataset(
      Source.fromFile(s"spark/s/${isin}.csv").getLines.toList.drop(3)
    )
    Context.spark.read.
      option("header", value = true).
      option("sep", ",").
      option("dateFormat", "dd-MM-yyyy").
      csv(ds)
  }

  def loadStock(isin: String, toTS: Timestamp, from: Date): List[String] = {
    try {
      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      val to = new Date(formatter.parse(formatter.format(toTS)).getTime)

      if (to.getTime > from.getTime) {
        println(s"Working for $isin from $from to $to (${from.getTime} , ${to.getTime})")

        //Euronext.refreshStock(isin, Option(from), Option(to))
        //Source.fromFile(s"spark/s/${isin}.csv").getLines.toList.drop(4)
        Euronext.dlStock(isin, Option(from), Option(to)).split('\n').toList.drop(4)
      } else {
        println(s"$isin already up to date")
        List.empty
      }
    }
    catch {
      case e: Throwable =>
        println(s"Error for $isin")
        println(e.toString)
        List.empty
    }
  }

  def consolidate(nb_stock: Integer = 0): Unit = {
    Context.spark.catalog.clearCache()

    val list = Euronext.getList().orderBy(desc("Turnover")).select($"ISIN", $"Last_DateTime".alias("to")).limit(nb_stock).repartition($"ISIN").cache() //.limit(20)
    list.show()

    // Gérer la non existence de la table
    val ds = if (Context.spark.catalog.tableExists("quotation")) {
      val lq = Database.lastQuotation
      val delta = list.join(lq, list("ISIN") === lq("ISIN"), "left").drop(lq("ISIN")).withColumnRenamed("last_quotation", "from").
        union(lq.join(list,list("ISIN") === lq("ISIN"),"leftanti").drop(list("ISIN")).withColumnRenamed("last_quotation","from").select($"isin",lit(null).alias("to"),$"from"))

      delta.flatMap {
        case Row(isin: String, toTS: Timestamp, from: Date) =>
          loadStock(isin, new Timestamp(toTS.getTime+ 24*60*60*1000), from) //new Timestamp(24*60*60*1000)
        case Row(isin: String, toTS: Timestamp, null) =>
          loadStock(isin, toTS, new Date(946681200000l))
        case Row(isin: String, null, from: Date) =>
          loadStock(isin, new Timestamp(Calendar.getInstance().getTime.getTime + 24*60*60*1000) , from)
      }
    }
    else {
      list.flatMap {
        case Row(isin: String, toTS: Timestamp) => loadStock(isin, toTS, new Date(946681200000l))
      }
    }

    //"ISIN","MIC","Date","Open","High","Low","Close","Number of Shares","Number of Trades","Turnover","Currency"
    //ISIN,"MIC","Date","Ouvert","Haut","Bas","Fermer","Nombre de titres","Number of Trades","Capitaux","Devise"
    val schema = StructType(Array(
      StructField("ISIN", StringType, false),
      StructField("MIC", StringType, false),
      StructField("Date", StringType, false), // DateType, false),
      StructField("Open", DoubleType, true), //DoubleType, true),
      StructField("High", DoubleType, true),
      StructField("Low", DoubleType, true),
      StructField("Close", DoubleType, true),
      StructField("Number_of_Shares", DoubleType, true),
      StructField("Number_of_Trades", DoubleType, true),
      StructField("Turnover", StringType, true),
      StructField("Currency", StringType, true)
    ))

    val load = Context.spark.
      read.
      option("sep", ",").
      option("timestampFormat", "dd/MM/yyyy").
      schema(schema).
      csv(ds).
      repartition($"ISIN").
      withColumn("Date", to_date($"Date", "dd/MM/yyyy")). // Plus tolérant
      withColumn("Turnover", $"Turnover".cast("Double")). // Plus tolérant
      cache()

    //withColumn("Day_Count", row_number().over(Window.partitionBy($"ISIN").orderBy($"Date"))).
    //load.write.mode(SaveMode.Overwrite).saveAsTable("quotation")

    //sql("select count(1) from quotation").show

    // Gérer la non existence de la table
    if (Context.spark.catalog.tableExists("quotation")) {
      println("Append to Table quotation")
      val lq = Database.lastQuotation
      val add = load.join(lq, load("ISIN") === lq("ISIN"), "left").drop(lq("ISIN")).filter($"last_quotation".isNull || $"Date" > $"last_quotation").drop($"last_quotation")
      println(s"Adding ${add.count} rows")
      add.repartition($"ISIN").write.mode(SaveMode.Append).saveAsTable("quotation")
    } else {
      println("Writing new Table quotation")
      load.write.mode(SaveMode.Overwrite).saveAsTable("quotation")
    }
    //parquet("spark/stock_value")
  }

}
