package net.lubet.spaif

object App extends App {
  println("SPAIF")

  //Euronext.getList().show()
  //Euronext.getList().printSchema()
  //val df1 = Euronext.getStock("FR0000031122",true)
  //val df2 = Euronext.getStock("FR0000076887",true)
  //val df = df1.unionAll(df2)
  //println("count:" + df.count())
  //df.show()
  Euronext.consolidate(100)
  Indicators.prepare
  Analyst.machinePrediction
  Context.spark.close
}
