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

case class Glicko2Rating(
  rating: Double = 1500.0,
  ratingDeviation: Double = 350.0,
  volatility: Double = 0.06,
  lastUpdated: Long = System.currentTimeMillis()
)

/** Full implementation of the Glicko-2 rating algorithm with proper volatility calculations
  */
class Glicko2 {
  private val TAU                   = 0.5
  private val CONVERGENCE_TOLERANCE = 0.000001

  def updateRating(player: Glicko2Rating, opponents: Seq[Glicko2Rating], outcomes: Seq[Double]): Glicko2Rating = {
    require(opponents.length == outcomes.length, "Opponents and outcomes must have same length")

    if (opponents.isEmpty) {
      // No games played, just increase RD due to time
      return player.copy(ratingDeviation =
        min(350.0, sqrt(player.ratingDeviation * player.ratingDeviation + player.volatility * player.volatility))
      )
    }

    val phi = player.ratingDeviation / 400.0
    val mu  = (player.rating - 1500.0) / 400.0

    // Step 2: Calculate v (estimated variance)
    val v = 1.0 / opponents
      .zip(outcomes)
      .map { case (opp, _) =>
        val phiJ   = opp.ratingDeviation / 400.0
        val muJ    = (opp.rating - 1500.0) / 400.0
        val gPhiJ  = 1.0 / sqrt(1.0 + 3.0 * phiJ * phiJ / (Pi * Pi))
        val eScore = 1.0 / (1.0 + exp(-gPhiJ * (mu - muJ)))
        gPhiJ * gPhiJ * eScore * (1.0 - eScore)
      }
      .sum

    val agg_score_differential =
      opponents
        .zip(outcomes)
        .map { case (opp, outcome) =>
          val phiJ   = opp.ratingDeviation / 400.0
          val muJ    = (opp.rating - 1500.0) / 400.0
          val gPhiJ  = 1.0 / sqrt(1.0 + 3.0 * phiJ * phiJ / (Pi * Pi))
          val eScore = 1.0 / (1.0 + exp(-gPhiJ * (mu - muJ)))
          gPhiJ * (outcome - eScore)
        }
        .sum

    // Step 3: Calculate delta (estimated improvement)
    val delta = v * agg_score_differential

    // Step 4: Calculate new volatility
    val sigma = player.volatility
    val a     = log(sigma * sigma)
    val f     = (x: Double) => {
      val ex    = exp(x)
      val num   = ex * (delta * delta - phi * phi - v - ex)
      val denom = 2.0 * pow(phi * phi + v + ex, 2.0)
      num / denom - (x - a) / (TAU * TAU)
    }

    val newVolatility = calculateNewVolatility(a, f, delta, phi, v)

    // Step 5: Update rating deviation
    val phiStar = sqrt(phi * phi + newVolatility * newVolatility)
    val newPhi  = 1.0 / sqrt(1.0 / (phiStar * phiStar) + 1.0 / v)

    // Step 6: Update rating
    val newMu = mu + newPhi * newPhi * agg_score_differential

    Glicko2Rating(
      rating = newMu * 400.0 + 1500.0,
      ratingDeviation = newPhi * 400.0,
      volatility = newVolatility,
      lastUpdated = System.currentTimeMillis()
    )
  }

  private def calculateNewVolatility(a: Double, f: Double => Double, delta: Double, phi: Double, v: Double): Double = {
    var A = a
    var B = if (delta * delta > phi * phi + v) {
      log(delta * delta - phi * phi - v)
    } else {
      var k = 1.0
      while (f(a - k * TAU) < 0) k += 1
      a - k * TAU
    }

    var fA = f(A)
    var fB = f(B)

    while (abs(B - A) > CONVERGENCE_TOLERANCE) {
      val C  = A + (A - B) * fA / (fB - fA)
      val fC = f(C)

      if (fC * fB < 0) {
        A = B
        fA = fB
      } else {
        fA = fA / 2.0
      }

      B = C
      fB = fC
    }

    exp(A / 2.0)
  }
}
