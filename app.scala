import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object Main extends App {
  val directory = "data"
  val files = Files.walk(Paths.get(directory)).iterator().asScala
  for (f <- files.map(_.toFile).filter(_.isFile)) {
    val lines = scala.io.Source.fromFile(f.getPath)
    val linesCount = lines.getLines().length
    println(s"$f : $linesCount lines")
  }
}
