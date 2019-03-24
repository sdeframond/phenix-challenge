import org.scalatest._
import java.nio.file.{Files, Path}
import java.io.File
import scala.collection.JavaConversions._
import scala.reflect.io.Directory

class MainSpec extends FunSpec {

  describe("Error cases") {
    describe("when a transaction refers to a non-existent reference") {
      it("logs an error") (pending)
      it("skips the transaction") (pending)
    }

    describe("when a transaction line is blank") {
      it("logs an error") (pending)
      it("skips the transaction") (pending)
    }

    describe("when the current day's transaction file is not found") {
      it("logs an error") (pending)
      it("crashes") (pending)
    }

    describe("when one of the current day's reference file is not found") {
      it("logs an error") (pending)
      it("skips the store for today and for the 7 days") (pending)
    }

    describe("when one of the 6 previous days' transaction file is not found") {
      it("logs an error") (pending)
      it("skips that day") (pending)
    }

    describe("when one of the 6 previous days' reference file is not found") {
      it("logs an error") (pending)
      it("skips the store for the whole 7 days") (pending)
    }

    describe("when the data directory does not exist") (pending)
    describe("when the data directory is not readable") (pending)

    describe("when output directory is not found") {
      it("logs an error") (pending)
      it("crashes") (pending)
    }

    describe("when output directory is not writable") {
      it("logs an error") (pending)
      it("crashes") (pending)
    }
  }

  describe("When things go well") {
    listFiles("src/test/resources/cases").forEach {
      caseDir => {
        val expectedFiles = listFiles(s"$caseDir/expectedResults")

        describe(s"${caseDir.getFileName}") {

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
  }

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
