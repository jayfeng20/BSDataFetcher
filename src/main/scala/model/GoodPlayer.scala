package model

/** A case class representing a good player with their tag, last-seen date (based on the last battle they played), and
  * custom rating. Location of files: data/good_players/...
  *
  * @param tag:
  *   Player's unique tag
  * @param lastSeen:
  *   Date when the player was last seen
  * @param rating:
  *   Player's rating
  */

case class GoodPlayer(tag: String, lastSeen: String, rating: Int)
