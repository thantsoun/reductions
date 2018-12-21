package reductions

import common._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig: MeasureBuilder[Unit, Double] = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer new Warmer.Default

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` if the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    def balanceIter(chars: Array[Char], leftParenthesis: Int): Boolean = {
      if (chars.isEmpty && leftParenthesis == 0) true
      else if (chars.isEmpty && leftParenthesis > 0) false
      else if (leftParenthesis < 0) false
      else {
        val head = chars.head
        val tail = chars.tail
        if (head == '(') balanceIter(tail, leftParenthesis + 1)
        else if (head == ')') balanceIter(tail, leftParenthesis - 1)
        else balanceIter(tail, leftParenthesis)
      }
    }

    balanceIter(chars, 0)
  }

  /** Returns `true` if the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, totalBalance: Int, minBalance: Int): (Int, Int) = {
      if (idx >= until) (totalBalance, minBalance)
      else {
        val (newTotalBalance, newMinBalance) = chars(idx) match {
          case '(' =>
            (totalBalance + 1, minBalance)
          case ')' =>
            val x = if (totalBalance - 1 < minBalance) totalBalance - 1 else minBalance
            (totalBalance - 1, x)
          case _ => (totalBalance, minBalance)
        }
        traverse(idx + 1, until, newTotalBalance, newMinBalance)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val middle = from + (until - from) / 2
        val (resLeft, resRight) = parallel(reduce(from, middle), reduce(middle, until))
        (resLeft._1 + resRight._1, Math.min(resLeft._2, resLeft._1 + resRight._2))
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
