import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object Main extends App {
  /*
    ## Assumptions

      - Less than 10 000 stores
      - Less than 1 million references per store
      - Less than 10 million unique products
      - Less than 10 million tx/j
      - Store references are ordered by productId in the files
      - For a given product, its ID stays the same in any price reference file.
          Note that this seems inconsistent with the example data : prices for a
          given product vary wildly from one store to the next. I am assuming
          this is because the example data is generated randomly.

    ## Goal

    To have an algorithm that computes the top 100 best sellers in term of
    quantity and revenue for each day, store and overall; and for a 7 days
    sliding windows as well.
    For any N as per the assumptions above, this algorithm should ideally
    have a worst-case complexity of:
      - O(1) for memory space
      - O(Nlog(N)) for disk space
      - O(Nlog(N)) for disk reads (as in # of lines read from disk, randomly or
        sequentially)
      - O(Nlog(N)) for time
    and be paralellizable

    ## Algorithm

    For the last day:
      Split transactions file per store into tmp files. Sort them by product.
      For each store:
        - sum quantities by product
        - join the results with the price references into 2 separate temporary files
          - productQties_<STORE_ID>.tmp.data
          - productRevenues_<STORE_ID>.tmp.data
        - merge the above files into 2 additional temporary files
          - productQties_GLOBAL_YYYYMMDD.tmp.data
          - productRevenues_GLOBAL_YYYYMMDD.tmp.data

    From these temporary files, produce:
      - top_100_ventes_<ID_MAGASIN>_YYYYMMDD.data
      - top_100_ca_<ID_MAGASIN>_YYYYMMDD.data

    And merge the tmp files to get:
      - top_100_ventes_GLOBAL_YYYYMMDD.data
      - top_100_ca_GLOBAL_YYYYMMDD.data

    Then for each of the 6 previous days, produce the same temporary files and
    incrementally merge them with the previous ones. Delete the temporary files
    of the day once merged.

    Then get the final result per store from the temporary files
      - top_100_ventes_<ID_MAGASIN>_YYYYMMDD-J7.data
      - top_100_ca_<ID_MAGASIN>_YYYYMMDD-J7.data

    Then merge the temporary files to get the overall final results:
      - top_100_ventes_GLOBAL_YYYYMMDD-J7.data
      - top_100_ca_GLOBAL_YYYYMMDD-J7.data.

    Note that each merge, sum and sort operation may be implemented using an
    external memory algorithm (for example using map reduce) for better scalability.

  */

  if(args.length != 1) {
    println("USAGE : phenix-challenge <path_to_data>")
    System.exit(1)
  }
  val directory = args(0)
  val files = Files.walk(Paths.get(directory)).iterator().asScala
  for (f <- files.map(_.toFile).filter(_.isFile)) {
    val lines = scala.io.Source.fromFile(f.getPath)
    val linesCount = lines.getLines().length
    println(s"$f : $linesCount lines")
  }
}
