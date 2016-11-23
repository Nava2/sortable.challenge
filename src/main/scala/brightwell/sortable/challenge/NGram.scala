package brightwell.sortable.challenge

import com.google.common.base.{CharMatcher, Splitter}

/**
  * Defines methods common for an NGram
  *
  * @param parts Components underlying an NGram
  */
case class NGram(parts: Seq[CharSequence]) {

  /**
    * Checks for a `needle` (this) in a `haystack`. Comparison happens by checking the contents of an NGram for
    * a common sequence of values. If the longest common sequence is more than 80% of the *part* of the NGram,
    * then it will be considered matched. If all parts of the NGram are matched, the NGram is matched.
    *
    * @param haystack Value to check against
    * @return The percentage of `this` NGram matched
    */
  def check(haystack: NGram) = {
    val result = if (parts.length > haystack.parts.length ||
      parts.isEmpty || haystack.parts.isEmpty) {
      0.0
    } else {
      haystack.parts.sliding(parts.length)
        .map(check =>
          parts.zip(check)
            .map {
              case (n, c) =>
                Distance.calcLongestCommonSC(c, n) match {
                  case Some(lcs) =>
                    if ((1.0 * c.length - lcs.length) / c.length > 0.8) {
                      1.0
                    } else {
                      0.0
                    }
                  case None => 0.0
                }
            }
            .sum
        )
        .max / parts.length
    }

    result
  }

  def ++(other: NGram) = NGram(parts ++ other.parts)

  def isEmpty = parts.isEmpty
  def nonEmpty = !isEmpty


}

/**
  * Collection ngram-ish utility functions.
  */
object NGram {

  /**
    * Get an empty NGram
    * @return
    */
  def empty = NGram(Seq())

  /**
    * `CharMatcher` for anything that is not a space or alphanum.
    */
  private val INVALID_LETTER_MATCHER = CharMatcher.JAVA_LETTER_OR_DIGIT.or(CharMatcher.WHITESPACE).negate()

  /**
    * Splitter to change a CharSequence into a collection of words for an
    * NGram-based on punctuation and whitespace.
    */
  private val WS_SPLITTER = Splitter.on(CharMatcher.BREAKING_WHITESPACE.or(CharMatcher.anyOf(",:().")))
                                    .omitEmptyStrings()
                                    .trimResults()

  /**
    * Filters a CharSequence to remove the characters listed in #INVALID_LETTER_MATCHER.
    * @param s Sequence to "clean-up" for matching
    * @return Cleaned up sequence and additionally lower-case.
    */
  def filterChars(s: CharSequence) = {
    INVALID_LETTER_MATCHER.removeFrom(s).toLowerCase
  }

  /**
    * Splits a char sequence into a Seq of String values
    * @param s Input to split and filter
    * @return Seq of filtered values, an "NGram"
    */
  def from(s: CharSequence) = {
    import collection.JavaConverters._

    val result = WS_SPLITTER.split(filterChars(s))
      .asScala.toSeq

    NGram(result)
  }



}
