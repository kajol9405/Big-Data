package com.kajol.example

object Sentiment extends Enumeration {
  type Sentiment = Value
  val POSITIVE, NEGATIVE, NEUTRAL = Value

  def toSentiment(sentiment: Int): Sentiment = sentiment match {
    case y if y == 0 || y == 1 => NEGATIVE
    case 2 => NEUTRAL
    case y if y == 3 || y == 4 => POSITIVE
  }
}