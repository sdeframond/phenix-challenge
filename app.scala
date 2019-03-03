object Main extends App {
  val lines = scala.io.Source.fromFile("data/reference_prod-10f2f3e6-f728-41f3-b079-43b0aa758292_20170514.data")
  lines.getLines() foreach println
}
