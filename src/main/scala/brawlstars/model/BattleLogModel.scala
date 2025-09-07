package brawlstars.model

object BattleLogModel {
  case class BattleLogResponse(items: List[Battle], paging: Paging)

  case class Paging(cursors: Cursors)

  case class Cursors(before: Option[String], after: Option[String])

  case class Battle(battleTime: String, battle: BattleResult, event: Event)

  case class BattleResult(
    mode: GameMode,
    `type`: GameType,
    result: GameResult,
    duration: Int,
    starPlayer: Option[Player],
    teams: Option[List[List[Player]]]
  )

  case class Player(tag: String, name: String, brawler: Brawler)

  case class Brawler(id: Long, name: String, power: Int, trophies: Int)

  case class Event(id: Long, mode: String, map: String)
}

val a = """
  |    {
  |      "battleTime": "20250907T065214.000Z",
  |      "event": {
  |        "id": 15000022,
  |        "mode": "bounty",
  |        "map": "Hideout"
  |      },
  |      "battle": {
  |        "mode": "bounty",
  |        "type": "soloRanked",
  |        "result": "defeat",
  |        "duration": 120,
  |        "starPlayer": {
  |          "tag": "#9QGU9P8R2",
  |          "name": "legoat",
  |          "brawler": {
  |            "id": 16000065,
  |            "name": "MANDY",
  |            "power": 11,
  |            "trophies": 17
  |          }
  |        },
  |        "teams": [
  |          [
  |            {
  |              "tag": "#2QQRLCGYVR",
  |              "name": "PantsLilTight",
  |              "brawler": {
  |                "id": 16000027,
  |                "name": "8-BIT",
  |                "power": 11,
  |                "trophies": 17
  |              }
  |            },
  |            {
  |              "tag": "#9QGU9P8R2",
  |              "name": "legoat",
  |              "brawler": {
  |                "id": 16000065,
  |                "name": "MANDY",
  |                "power": 11,
  |                "trophies": 17
  |              }
  |            },
  |            {
  |              "tag": "#LPL2PGV0",
  |              "name": "¤Key¤",
  |              "brawler": {
  |                "id": 16000003,
  |                "name": "BROCK",
  |                "power": 11,
  |                "trophies": 17
  |              }
  |            }
  |          ],
  |          [
  |            {
  |              "tag": "#JG8L2L2",
  |              "name": "Sleep Mode",
  |              "brawler": {
  |                "id": 16000046,
  |                "name": "BELLE",
  |                "power": 11,
  |                "trophies": 17
  |              }
  |            },
  |            {
  |              "tag": "#8YVPLGUU",
  |              "name": "Matisito4.1",
  |              "brawler": {
  |                "id": 16000021,
  |                "name": "GENE",
  |                "power": 11,
  |                "trophies": 17
  |              }
  |            },
  |            {
  |              "tag": "#2GQYRVCJR",
  |              "name": "GR| ALEXCKO🐶✨",
  |              "brawler": {
  |                "id": 16000061,
  |                "name": "GUS",
  |                "power": 11,
  |                "trophies": 17
  |              }
  |            }
  |          ]
  |        ]
  |      }
  |    },
  |""".stripMargin
