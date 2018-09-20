package net.lubet.spaif


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

}
