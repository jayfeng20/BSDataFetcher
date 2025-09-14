package model

import java.sql.Timestamp
//import java.time.OffsetDateTime

/** A case class representing a good player with their tag, last-seen date (based on the last battle they played), and
  * custom rating. Location of files: data/good_players/...
  */
case class GoodPlayerSummary(
  tag: String,
  rank: Int,              // 1-1000 ranking
  glicko2Rating: Double,  // Current Glicko-2 rating
  glicko2RD: Double,      // Rating deviation (uncertainty)
  trueSkillMu: Double,    // TrueSkill skill estimate
  trueSkillSigma: Double, // TrueSkill uncertainty
  combinedRating: Double, // Final weighted rating
  battles: Long,          // Total battles
  winRate: Double,        // Win percentage (0.0-1.0)
  lastSeen: Timestamp,    // Last activity
  daysActive: Long        // Days between first and last seen)
)
