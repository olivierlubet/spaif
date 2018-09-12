package net.lubet.spaif

//import net.lubet.spaif._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs._

object SQL {
    
  import Context.spark.implicits._
  import Context.spark._
  import Context._
  
  def init ={
      sql("""
      DROP TABLE IF EXISTS indicator
      """)
      
      sql("""
      CREATE TABLE  if not exists indicator
      ( Date date, Value double)
      PARTITIONED BY (ISIN string, Type string)
      """)
  }
  
  def prepare = {
      sql("""
      insert into indicator
      select ISIN,Date,"Open" as Type, Open as Value
      from quotation
      where ISIN in ("FR0000045072","FR0000130809")
      """)
      
      sql("""
      SELECT ISIN,Date,"MA10" as Type,
       AVG(Open) OVER (ORDER BY ISIN,Date ASC ROWS 9 PRECEDING) AS MA10
        FROM   indicator
      """)
      
      ma(5).write.mode(SaveMode.Append).partitionBy("isin","type").saveAsTable("indicator")
      
      
      scala>       sql("""
     |       select * from indicator
     |       """).groupBy("isin","date").pivot("type",Seq("MA5","MA10")).agg(sum($"value")).show
  }
  
  scala>   def ma(delay:Integer):DataFrame={
     |    sql(s"""
     |       SELECT i.ISIN,i.Date,"MA${delay}" as type,
     |        AVG(i.value)
     |        OVER (PARTITION BY i.isin ORDER BY i.date ASC ROWS ${delay-1} PRECEDING) AS value
     |         FROM   indicator i
     |         LEFT JOIN indicator ai on (i.isin=ai.isin and ai.type="MA${delay}" and i.date=ai.date)
     |         where i.type="Close" and ai.isin is null
     |       """)
     |   }
}