package brawlstars.model

enum GameResult {
  case victory
  case defeat
  case draw
}

object GameResult:

  import io.circe.{Decoder, Encoder}

  private def fromApiString(s: String): GameResult = s match
    case "victory" => GameResult.victory
    case "defeat"  => GameResult.defeat
    case "draw"    => GameResult.draw
    case _         => throw new Exception(s"Unknown game result: $s")

  implicit val decodeGameResult: Decoder[GameResult] = Decoder.decodeString.emap { str =>
    try
      Right(fromApiString(str))
    catch {
      case e: Exception => Left(e.getMessage)
    }
  }

  implicit val encodeGameResult: Encoder[GameResult] = Encoder.encodeString.contramap {
    case GameResult.victory => "victory"
    case GameResult.defeat  => "defeat"
    case GameResult.draw    => "draw"
  }
