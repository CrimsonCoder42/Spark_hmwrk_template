package org.cscie88c.week2

// write code for class Administrator below

object Administrator extends App {
  val streamLower = LazyList.from(('a' to 'z').iterator)
  val streamUpper = LazyList.from(('A' to 'Z').iterator)
  val streamLowerUpper = streamLower.zip(streamUpper)

}
