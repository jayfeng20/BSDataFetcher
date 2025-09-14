package model

import pipeline.gpa.{Glicko2Rating, TrueSkillRating}

import java.sql.Timestamp

case class PlayerProfile(
  tag: String,                // Unique player identifier
  glicko2: Glicko2Rating,     // Full Glicko-2 state
  trueSkill: TrueSkillRating, // Full TrueSkill state
  battles: Long,              // Total battles played
  wins: Long,                 // Total wins
  losses: Long,               // Total losses
  draws: Long,                // Total draws
  lastSeen: Timestamp,        // Last battle timestamp
  firstSeen: Timestamp,       // First battle timestamp
  combinedRating: Double,     // Weighted rating score
  isActive: Boolean           // Active in last 30 days
)
