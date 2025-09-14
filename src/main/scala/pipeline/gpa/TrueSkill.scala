package pipeline.gpa

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import scala.math._

case class TrueSkillRating(
  mu: Double = 25.0,
  sigma: Double = 25.0 / 3.0,
  lastUpdated: Long = System.currentTimeMillis()
)

/** Implementation of Microsoft's TrueSkill algorithm for multiplayer scenarios
  */
class TrueSkill {
  private val BETA             = 25.0 / 6.0 // skill class width
  private val DRAW_PROBABILITY = 0.10       // 10% draw probability
  private val EPSILON          = drawMargin(DRAW_PROBABILITY, BETA)

  def drawMargin(drawProbability: Double, beta: Double): Double =
    // Inverse CDF approximation for draw margin
    sqrt(2.0) * beta * inverseErrorFunction(drawProbability / 2.0)

  private def inverseErrorFunction(x: Double): Double = {
    // Approximation of inverse error function
    val a    = 8.0 * (Pi - 3.0) / (3.0 * Pi * (4.0 - Pi))
    val sign = if (x < 0) -1.0 else 1.0
    val y    = log(1.0 - x * x)
    val z    = 2.0 / (Pi * a) + y / 2.0
    sign * sqrt(sqrt(z * z - y / a) - z)
  }

  def updateRating(winner: TrueSkillRating, loser: TrueSkillRating): (TrueSkillRating, TrueSkillRating) = {
    val c = sqrt(2.0 * BETA * BETA + winner.sigma * winner.sigma + loser.sigma * loser.sigma)

    val winnerMean = winner.mu
    val loserMean  = loser.mu
    val delta      = winnerMean - loserMean

    val v = vFunction(delta / c, EPSILON / c)
    val w = wFunction(delta / c, EPSILON / c)

    val winnerSigmaSquared = winner.sigma * winner.sigma
    val loserSigmaSquared  = loser.sigma * loser.sigma

    val newWinnerMu = winnerMean + (winnerSigmaSquared / c) * v
    val newLoserMu  = loserMean - (loserSigmaSquared / c) * v

    val newWinnerSigma = sqrt(winnerSigmaSquared * (1.0 - (winnerSigmaSquared / (c * c)) * w))
    val newLoserSigma  = sqrt(loserSigmaSquared * (1.0 - (loserSigmaSquared / (c * c)) * w))

    (
      TrueSkillRating(newWinnerMu, newWinnerSigma, System.currentTimeMillis()),
      TrueSkillRating(newLoserMu, newLoserSigma, System.currentTimeMillis())
    )
  }

  private def vFunction(t: Double, epsilon: Double): Double = {
    val denom = cumulativeDistribution(t - epsilon) - cumulativeDistribution(t + epsilon)
    if (abs(denom) < 1e-10) 0.0
    else (densityFunction(t - epsilon) - densityFunction(t + epsilon)) / denom
  }

  private def wFunction(t: Double, epsilon: Double): Double = {
    val v = vFunction(t, epsilon)
    v * (v + t - epsilon + t + epsilon)
  }

  private def densityFunction(x: Double): Double =
    (1.0 / sqrt(2.0 * Pi)) * exp(-0.5 * x * x)

  private def cumulativeDistribution(x: Double): Double =
    0.5 * (1.0 + erf(x / sqrt(2.0)))

  private def erf(x: Double): Double = {
    // Approximation of error function
    val sign = if (x >= 0) 1.0 else -1.0
    val absX = abs(x)
    val a1   = 0.254829592
    val a2   = -0.284496736
    val a3   = 1.421413741
    val a4   = -1.453152027
    val a5   = 1.061405429
    val p    = 0.3275911
    val t    = 1.0 / (1.0 + p * absX)
    val y    = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * exp(-absX * absX)
    sign * y
  }
}
