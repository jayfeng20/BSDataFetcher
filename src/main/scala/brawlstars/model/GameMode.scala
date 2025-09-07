package brawlstars.model

enum GameMode {
  case soloShowdown
  case duoShowdown
  case heist
  case bounty
  case siege
  case gemGrab
  case brawlBall
  case bigGame
  case bossFight
  case roboRumble
  case takedown
  case loneStar
  case presentPlunder
  case hotZone
  case superCityRampage
  case knockout
  case volleyBrawl
  case basketBrawl
  case holdTheTrophy
  case trophyThieves
  case duels
  case wipeout
  case payload
  case botDrop
  case hunters
  case lastStand
  case snowtelThieves
  case pumpkinPlunder
  case trophyEscape
  case wipeout5V5
  case knockout5V5
  case gemGrab5V5
  case brawlBall5V5
  case godzillaCitySmash
  case paintBrawl
  case trioShowdown
  case zombiePlunder
  case jellyfishing
  case unknown
}

import io.circe.{Decoder, Encoder}

object GameMode:

  private def fromApiString(s: String): GameMode = s match
    case "soloShowdown"      => GameMode.soloShowdown
    case "duoShowdown"       => GameMode.duoShowdown
    case "heist"             => GameMode.heist
    case "bounty"            => GameMode.bounty
    case "siege"             => GameMode.siege
    case "gemGrab"           => GameMode.gemGrab
    case "brawlBall"         => GameMode.brawlBall
    case "bigGame"           => GameMode.bigGame
    case "bossFight"         => GameMode.bossFight
    case "roboRumble"        => GameMode.roboRumble
    case "takedown"          => GameMode.takedown
    case "loneStar"          => GameMode.loneStar
    case "presentPlunder"    => GameMode.presentPlunder
    case "hotZone"           => GameMode.hotZone
    case "superCityRampage"  => GameMode.superCityRampage
    case "knockout"          => GameMode.knockout
    case "volleyBrawl"       => GameMode.volleyBrawl
    case "basketBrawl"       => GameMode.basketBrawl
    case "holdTheTrophy"     => GameMode.holdTheTrophy
    case "trophyThieves"     => GameMode.trophyThieves
    case "duels"             => GameMode.duels
    case "wipeout"           => GameMode.wipeout
    case "payload"           => GameMode.payload
    case "botDrop"           => GameMode.botDrop
    case "hunters"           => GameMode.hunters
    case "lastStand"         => GameMode.lastStand
    case "snowtelThieves"    => GameMode.snowtelThieves
    case "pumpkinPlunder"    => GameMode.pumpkinPlunder
    case "trophyEscape"      => GameMode.trophyEscape
    case "wipeout5V5"        => GameMode.wipeout5V5
    case "knockout5V5"       => GameMode.knockout5V5
    case "gemGrab5V5"        => GameMode.gemGrab5V5
    case "brawlBall5V5"      => GameMode.brawlBall5V5
    case "godzillaCitySmash" => GameMode.godzillaCitySmash
    case "paintBrawl"        => GameMode.paintBrawl
    case "trioShowdown"      => GameMode.trioShowdown
    case "zombiePlunder"     => GameMode.zombiePlunder
    case "jellyfishing"      => GameMode.jellyfishing
    case _                   => GameMode.unknown

  given Decoder[GameMode] = Decoder.decodeString.map(fromApiString)

  given Encoder[GameMode] = Encoder.encodeString.contramap[GameMode](_.toString)

enum RankedGameMode {
  case Heist
  case Bounty
  case GemGrab
  case BrawlBall
  case Knockout
  case HotZone
  case Other
}
