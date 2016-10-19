package brightwell.sortable.challenge

import scala.collection.mutable
import java.lang.StringBuilder

case class Distance(s: CharSequence, equalThreshold: Double) {

  def check(seq: CharSequence): Double = {
    val dist = 1.0 * Distance.calc(this.s, seq)
    val totalLen = Math.max(this.s.length, seq.length)

    dist / totalLen
  }

  override def equals(o: scala.Any): Boolean = o match {
    case that: Distance =>

      val check = this.check(that.s)

      val thresh = Math.min(this.equalThreshold, that.equalThreshold)

      check <= thresh

    case that: CharSequence =>
      val check = this.check(that)
      check <= this.equalThreshold

    case _ => false
  }
}

/**
  * Created by kevin on 15/10/2016.
  */
object Distance {

  private def min3(a: Int, b: Int, c: Int) = {
    Math.min(Math.min(a, b), c)
  }

  def calc(leftSeq: CharSequence, rightSeq: CharSequence): Int = {
    val left = leftSeq.codePoints().toArray
    val right = rightSeq.codePoints().toArray

    val M = left.length + 1
    val N = right.length + 1

    // Create an MxN matrix with the first row/column initialized
    val matrix: mutable.Seq[mutable.Seq[Int]] = mutable.Seq.tabulate[Int](M, N)((i, j) => {
      if (i == 0) {
        j
      } else if (j == 0) {
        i
      } else {
        0
      }
    })

    for {
      i <- 1 until M
      lChar = left(i-1)
      j <- 1 until N
      rChar = right(j-1)
    } {
      val cost = if (lChar == rChar) 0 else 1
      matrix(i)(j) = min3(matrix(i-1)(j) + 1, matrix(i)(j-1) + 1, matrix(i-1)(j-1) + cost)
    }

    matrix(M-1)(N-1)
  }

  def calcLongestCommonSC(l: CharSequence, r: CharSequence, checkPercent: Double = 0.20): Option[CharSequence] = {
    val left = l.codePoints().toArray
    val right = r.codePoints().toArray

    val M = left.length + 1
    val N = right.length + 1

    // Create an MxN matrix with the first row/column initialized
    val matrix = mutable.Seq.fill[Int](M, N)(0)

    for {
      i <- 1 until M
      lChar = left(i-1)
      j <- 1 until N
      rChar = right(j-1)
    } {
      val v = if (lChar == rChar) {
          matrix(i - 1)(j - 1) + 1
        } else {
          Math.max(matrix(i - 1)(j), matrix(i)(j - 1))
        }

      matrix(i)(j) = v
    }

    // Check so we can short circuit and save building the string via
    // the reverse tracking
    if ((1.0 * matrix(M-1)(N-1)) / M <= checkPercent) {
      val outSeqBuf = new StringBuilder(Math.max(l.length, r.length))

      var prevIdx = N+1
      for {
        i <- (M-1) to 1 by -1
        col = matrix(i) if prevIdx > 1
      } {
        prevIdx = col.zipWithIndex
          .take(prevIdx)
          .maxBy(_._1)._2

        if (prevIdx > 0)
          outSeqBuf.appendCodePoint(right(prevIdx - 1))
      }

      Some(outSeqBuf.reverse.toString)
    } else {
      None
    }


  }

  def calcP(l: CharSequence, r: CharSequence) = {
    val diff = Math.abs(l.length - r.length)
    val max = 1.0 * Math.max(l.length, r.length)

    val result = if (diff / max > 0.6) {
      // short circuit, there's no point checking if gt
      diff / max
    } else {
      calc(l, r) / max
    }

    result
  }

}
