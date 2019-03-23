import org.scalatest._
import java.nio.file.{Files, Path}
import java.io.File
import scala.collection.JavaConversions._
import scala.reflect.io.Directory

class MainSpec extends FunSpec {

  listFiles("src/test/resources/cases").forEach {
    caseDir => {
      val expectedFiles = listFiles(s"$caseDir/expectedResults")
      describe(caseDir.getFileName.toString) {
        it("should produce all expected files") {
          withTempDirectory {
            outputPath => {
              Main.main(Array(s"$caseDir/data", outputPath.toString, "2017-05-14"))
              val expected = expectedFiles.map(_.getFileName).toList.sorted
              val results = listFiles(outputPath.toString).map(_.getFileName).sorted
              assert(expected === results)
            }
          }
        }
        expectedFiles forEach {
          expected => it(s"${expected.getFileName} should have the expected content") {
            withTempDirectory {
              outputPath => {
                Main.main(Array(s"$caseDir/data", outputPath.toString, "2017-05-14"))
                val expectedContent = scala.io.Source.fromFile(expected.toFile).getLines.toList
                val resultContent = scala.io.Source.fromFile(s"${outputPath}/${expected.getFileName}").getLines.toList
                assert(expectedContent === resultContent)
              }
            }
          }
        }
      }
    }
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def withTempDirectory[B](f: Path => B): Unit = {
    val tmpPath = Files.createTempDirectory("phenixtest")
    try { f(tmpPath) } finally {
      val directory = new Directory(new File(tmpPath.toString))
      directory.deleteRecursively()
    }
  }

  def listFiles(dirName: String): List[Path] = {
    val file = new File(dirName)
    file.listFiles.toList.map(_.toPath)
  }
}
