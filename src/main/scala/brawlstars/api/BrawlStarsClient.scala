package brawlstars.api

import sttp.client3._
import sttp.client3.circe._
import io.circe.generic.auto._
import brawlstars.model.BattleLogModel.BattleLogResponse

class BrawlStarsClient(apiToken: String) {
  private val baseUrl = "https://api.brawlstars.com/v1"
  private val backend = HttpURLConnectionBackend() // simplest blocking backend

  def fetchBattleLog(playerTag: String): Either[String, BattleLogResponse] = {
    val request = basicRequest
      .header("Authorization", s"Bearer $apiToken")
      .get(uri"$baseUrl/players/$playerTag/battlelog")
      .response(asJson[BattleLogResponse])

    val response = request.send(backend)

    response.body match
      case Right(battleLog) => Right(battleLog)
      case Left(err)        => Left(s"Failed to fetch battle log: ${err.getMessage}")
  }
}
