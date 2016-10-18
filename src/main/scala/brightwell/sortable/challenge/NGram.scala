package brightwell.sortable.challenge

import com.google.common.base.{CharMatcher, Splitter}

/**
  * Created by kevin on 16/10/2016.
  */
object NGram {

  private val invalidLetters = CharMatcher.JAVA_LETTER_OR_DIGIT.negate()

  private val WS_SPLITTER = Splitter.on(CharMatcher.BREAKING_WHITESPACE.or(CharMatcher.anyOf(",:().")))
                                    .omitEmptyStrings()
                                    .trimResults()

  def filterChars(s: CharSequence) = {
    invalidLetters.removeFrom(s).toLowerCase
  }

  def from(s: CharSequence) = {
    import collection.JavaConverters._

    WS_SPLITTER.split(s).asScala
      .map( filterChars )
      .filter( _.nonEmpty ) // get rid of empty chars
      .toSeq
  }

  def check(needle: Seq[String], haystack: Seq[String]) = {
    val result = if (needle.length > haystack.length ||
        needle.isEmpty || haystack.isEmpty) {
        0.0
      } else {
        (0 to (haystack.length - needle.length))
          .map( i => haystack.slice(i, i + needle.length) )
          .map(check =>
              needle.zip(check)
                .map {
                  case (n, c) => if (c == n) 1.0 else 0.0 //Distance.calcP(n, c)
                }
                .sum
          )
          .max / needle.length
      }

    result
  }

}
