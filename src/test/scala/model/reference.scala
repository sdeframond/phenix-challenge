package PhenixChallenge.model

import org.scalatest._
import scala.util.{Failure, Success, Try}

class ReferenceSpec extends FunSpec {
  describe(".parse") {
    List(
      ("1|10.10", Success(Reference(ProductId(1), 10.10))),
      ("1|10", Success(Reference(ProductId(1), 10))),
      ("1|10.1", Success(Reference(ProductId(1), 10.1)))
    ).foreach { case (input, expected) =>
      it(s"parses '$input' as expected") {
        val result = Reference.parse(input)
        assert(result === expected)
      }
    }

    it(s"fails to parse '1|10.111'") {
      val result = Reference.parse("1|10.111")
      assert(result.isFailure)
    }
  }
}
