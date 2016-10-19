package brightwell.sortable.challenge

import org.scalatest._

class DistanceTest extends FlatSpec with Matchers {

  "Distance.calc(\"a\", \"b\")" should "return 1" in {

    Distance.calc("a", "b") should be(1)

  }

  "Distance.calc(\"aaaa\", \"aab\")" should "return 2" in {

    Distance.calc("aaaa", "aab") should be(2)

  }

  "Distance.calcLongestCommon(\"aaaa\", \"aab\")" should "return 2" in {

    Distance.calcLongestCommonSC("abcda", "acbdea") should be("acda")

  }
}