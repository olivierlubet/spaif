package net.lubet.spaif

import scala.io._

import java.io
import java.io.{BufferedWriter, File, FileWriter}
import java.net.{URL, URLEncoder}
import java.text.SimpleDateFormat
import java.util.Date

import org.jsoup.nodes.{Document, Element}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import Context.spark.implicits._



object Euronext {
  def dlList(): String = {
    if (Context.mock) {
      """
        |"Name";"ISIN";"Symbol";"Market";"Trading Currency";"Open";"High";"Low";"Last";"Last Date/Time";"Time Zone";"Volume";"Turnover"
        |"European Equities";"";;;;;;;;;;;
        |"03 Sep 2018";;;;;;;;;;;;
        |"All datapoints provided as of end of last active trading day.";;;;;;;;;;;;
        |"1000MERCIS";"FR0010285965";"ALMIL";"Euronext Growth Paris";"EUR";"30.10";"30.30";"30.00";"30.30";"03/09/2018 15:58";"CET";"996";"30062.40"
        |"2CRSI";"FR0013341781";"2CRSI";"Euronext Paris";"EUR";"10.70";"10.85";"10.70";"10.75";"03/09/2018 16:51";"CET";"4386";"47202.50"
        |"2VALORISE";"BE0974275076";"VALOR";"Euronext Brussels";"EUR";"5.15";"5.15";"5.15";"5.15";"03/09/2018 16:38";"CET";"943";"4856.45"
        |"4SERVICE CLOUD";"CH0299791381";"MLOVE";"Euronext Access Paris";"EUR";"0.146";"0.146";"0.146";"0.146";"23/07/2018 12:09";"CET";"5200";"759.20"
        |"A TOUTE VITESSE";"FR0010050773";"MLATV";"Euronext Access Paris";"EUR";"2.00";"2.00";"2.00";"2.00";"23/08/2018 16:30";"CET";"40";"80.00"
        |"A.S.T. GROUPE";"FR0000076887";"ASP";"Euronext Paris";"EUR";"10.80";"11.22";"10.76";"11.06";"03/09/2018 17:35";"CET";"26635";"294569.38"
        |"AALBERTS INDUSTR";"NL0000852564";"AALB";"Euronext Amsterdam";"EUR";"36.82";"36.89";"35.77";"36.08";"03/09/2018 17:35";"CET";"384349";"13885851.55"
        |"AB INBEV";"BE0974293251";"ABI";"Euronext Brussels";"EUR";"80.25";"80.45";"79.43";"80.14";"03/09/2018 17:36";"CET";"1275549";"101932116.43"
        |"AB SCIENCE";"FR0010557264";"AB";"Euronext Paris";"EUR";"4.39";"4.41";"4.32";"4.33";"03/09/2018 17:35";"CET";"50534";"219684.38"
        |"ABC ARBITRAGE";"FR0004040608";"ABCA";"Euronext Paris";"EUR";"7.14";"7.14";"7.11";"7.14";"03/09/2018 17:35";"CET";"14090";"100389.10"
        |"ABC ORTHODONTICS";"CH0044947239";"MLABC";"Euronext Access Paris";"EUR";"0.29";"0.29";"0.29";"0.29";"04/04/2016 15:00";"CET";"1";"0.29"
      """.stripMargin
    } else {
      val url = new URL("https://www.euronext.com/en/popup/data/download?ml=nyx_pd_stocks&cmd=default&formKey=nyx_pd_filter_values%3A1006ef55d4998cc0fad71db6a6f38530")
      val data = Map("format" -> "2",
        "layout" -> "2",
        "decimal_separator" -> "1",
        "date_format" -> "1",
        "op" -> "Go",
        "form_id" -> "nyx_download_form")
      Browser.post(url, data)
    }

  }

  def refreshList = {
    val file = new File("spark/EuronextList.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(dlList)
    bw.close()
  }

  def getList(refresh: Boolean = false): DataFrame = {
    val today = new java.sql.Date(System.currentTimeMillis)

    if (refresh) refreshList
    
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
    
    Context.spark.read.
      option("header", value = false).
      option("sep", ";").
      option("timestampFormat","dd/MM/yyyy HH:mm").
      schema(schema).
      csv("spark/EuronextList.csv")
  }

  def dlStock(isin:String): String ={
    val name:String="No need"
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //https://www.euronext.com/nyx_eu_listings/price_chart/download_historical?typefile=csv&layout=vertical&typedate=dmy&separator=point&mic=XPAR&isin=FR0010478248&name=ATARI&namefile=Price_Data_Historical&from=1535328000000&to=1535932800000&adjusted=1&base=0
    val url = new URL(s"https://www.euronext.com/nyx_eu_listings/price_chart/download_historical?typefile=csv&layout=vertical&typedate=dmy&separator=point&mic=XPAR&isin=${isin}&name=${URLEncoder.encode(name, "UTF-8")}&namefile=Price_Data_Historical&from=946681200000&to=1535932800000&adjusted=1&base=0")
    println(url.toString)
    Browser.get(url)
  }

  def refreshStock(isin:String) = {
    val file = new File(s"spark/s/${isin}.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(dlStock(isin))
    bw.close()
  }

  def getStock(isin:String, refresh:Boolean=false):DataFrame = {
    if(refresh)  refreshStock(isin)
    
    
    val ds = Context.spark.createDataset(
      Source.fromFile(s"spark/s/${isin}.csv").getLines.toList.drop(3)
      )
    Context.spark.read.
      option("header", value = true).
      option("sep", ",").
      option("dateFormat","dd-MM-yyyy").
      csv(ds)
  }
  
  def consolidate(nb_stock:Integer=2):Unit = {
    val list = Euronext.getList().orderBy(desc("Turnover")).select("ISIN").limit(1000).repartition(20)
    list.show()
    val ds = list.flatMap{
      case Row(isin: String) =>
      try {
        println(s"Working for $isin")
        Euronext.refreshStock(isin)
        Source.fromFile(s"spark/s/${isin}.csv").getLines.toList.drop(4)
      } catch {
        case e: Throwable =>
          println(s"Error for $isin")
          println(e.toString)
          List.empty
      }
    }
    
    //"ISIN","MIC","Date","Open","High","Low","Close","Number of Shares","Number of Trades","Turnover","Currency"
    val schema = StructType(Array(
      StructField("ISIN", StringType, false),
      StructField("MIC", StringType, false),
      StructField("Date",StringType,false),// DateType, false),
      StructField("Open", DoubleType,true),//DoubleType, true),
      StructField("High", DoubleType, true),
      StructField("Low", DoubleType, true),
      StructField("Close", DoubleType, true),
      StructField("Number_of_Shares", DoubleType, true),
      StructField("Number_of_Trades", DoubleType, true),
      StructField("Turnover", StringType, true),
      StructField("Currency", StringType, true)
    ))
    
    val df = Context.spark.
      read.
      option("sep", ",").
      option("timestampFormat","dd/MM/yyyy").
      schema(schema).
      csv(ds)
    
    val df_f = df.
      withColumn("Date", to_date($"Date", "dd/MM/yyyy")). // Plus tolérant
      withColumn("Turnover", df("Turnover").cast("Double"))
      
    //
    df_f.repartition(4).write.
    mode("overwrite").
    parquet("spark/stock_value")
  }
}
