package net.lubet.spaif

import net.lubet.spaif.Indicators._


object App extends App {
  println(
    """
      |
      |  ___________________  _____  .______________
      | /   _____/\______   \/  _  \ |   \_   _____/
      | \_____  \  |     ___/  /_\  \|   ||    __)
      | /        \ |    |  /    |    \   ||     \
      |/_______  / |____|  \____|__  /___|\___  /
      |        \/                  \/         \/
      |
    """.stripMargin)

  Euronext.consolidate(200)
  Indicators.compute
  Analyst.learn
  Analyst.predict
  Indicators.export
  Context.spark.close

  time(add(performance("S", 15)))
}
