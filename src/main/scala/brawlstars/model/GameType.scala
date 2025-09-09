package brawlstars.model

enum GameType {
  case soloRanked // actual ranked games
  case ranked     // trophies games
  case tournament
  case friendly
}

object GameType:

  import io.circe.{Decoder, Encoder}

  private def fromApiString(s: String): GameType = s match
    case "soloRanked" => GameType.soloRanked
    case "ranked"     => GameType.ranked
    case "tournament" => GameType.tournament
    case "friendly"   => GameType.friendly
    case _            => throw new Exception(s"Unknown game type: $s")

  implicit val decodeGameType: Decoder[GameType] = Decoder.decodeString.emap { str =>
    try
      Right(fromApiString(str))
    catch {
      case e: Exception => Left(e.getMessage)
    }
  }

  implicit val encodeGameType: Encoder[GameType] = Encoder.encodeString.contramap {
    case GameType.soloRanked => "soloRanked"
    case GameType.ranked     => "ranked"
    case GameType.tournament => "tournament"
    case GameType.friendly   => "friendly"
  }
