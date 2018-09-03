package net.lubet.spaif

object App extends App {
  println("Hello")

  //Euronext.getList().show()
  Euronext.refreshStock("2CRSI","FR0013341781")
}
