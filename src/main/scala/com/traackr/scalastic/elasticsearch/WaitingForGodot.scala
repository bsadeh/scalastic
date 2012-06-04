package com.traackr.scalastic.elasticsearch

trait WaitingForGodot {
  self: Indexer =>

  val defaultSeed = 1
  val defaultMaxFactor = 64

  @deprecated(message="Replaced with waitTillCountAtMost", since="0.0.4")
  def catchUpOn(indices: Iterable[String] = Nil, `type`: String, target: Int, seed: Int = defaultSeed, maxFactor: Int = defaultMaxFactor) =
    waitTillCountAtMost(indices, `type`, target, seed, maxFactor)
    
  def exactly(current: Long, target: Long): Boolean = { current == target }
  def waitTillCountExactly(indices: Iterable[String] = Nil, `type`: String, target: Int, seed: Int = defaultSeed, maxFactor: Int = defaultMaxFactor) =
    waitTillCountMatches(indices, `type`, target, exactly _, "exactly", seed, maxFactor)

  def atLeast(current: Long, target: Long): Boolean = { current < target }
  def waitTillCountAtLeast(indices: Iterable[String] = Nil, `type`: String, target: Int, seed: Int = defaultSeed, maxFactor: Int = defaultMaxFactor) =
    waitTillCountMatches(indices, `type`, target, atLeast _, "at least", seed, maxFactor)

  def atMost(current: Long, target: Long): Boolean = { current > target }
  def waitTillCountAtMost(indices: Iterable[String] = Nil, `type`: String, target: Int, seed: Int = defaultSeed, maxFactor: Int = defaultMaxFactor) =
    waitTillCountMatches(indices, `type`, target, atMost _, "at most", seed, maxFactor)

  def waitTillCountMatches(indices: Iterable[String] = Nil, `type`: String, target: Int, f: (Long, Long) => Boolean, criteriaDescription: String, seed: Int = defaultSeed, maxFactor: Int = defaultMaxFactor) = {
    var factor = seed
    while (factor <= maxFactor && f(count(indices, types = Seq(`type`)), target)) {
      info("waiting on {} to count {} {} in {} sec ...", `type`, criteriaDescription, target, factor)
      Thread sleep factor * 1000
      factor *= 2
    }
    if (factor > maxFactor) throw new RuntimeException(
      "failed to wait on count of %s to match %s %s while indexing, after %s seconds".format(`type`, criteriaDescription, target, maxFactor))
  }
}