package pipeline.gpa

import brawlstars.model.BattleLogModel.Player
import pipeline.gpa.Glicko2Rating
import pipeline.gpa.TrueSkillRating

class RatingSystem {
  case class PlayerRating(
    tag: String,
    glicko2: Glicko2Rating,
    trueSkill: TrueSkillRating,
    battles: Long,
    lastSeen: java.sql.Timestamp,
    combinedRating: Double
  )
}
