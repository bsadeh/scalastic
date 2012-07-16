package org.elasticsearch.test.integration.search.scan

import org.elasticsearch.action.search._
import scala.collection.JavaConversions._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SearchScanScrollingTests extends IndexerBasedTest {

  override def shouldCreateDefaultIndex = false

  test("shard1docs100size3") {
    scroll(1, 100, 3)
  }

  test("shard1docs100size7") {
    scroll(1, 100, 7)
  }

  test("shard1docs100size13") {
    scroll(1, 100, 13)
  }

  test("shard1docs100size24") {
    scroll(1, 100, 24)
  }

  test("shard1docs100size45") {
    scroll(1, 100, 45)
  }

  test("shard1docs100size63") {
    scroll(1, 100, 63)
  }

  test("shard1docs100size89") {
    scroll(1, 100, 89)
  }

  test("shard1docs100size99") {
    scroll(1, 100, 99)
  }

  test("shard1docs100size100") {
    scroll(1, 100, 100)
  }

  test("shard1docs100size101") {
    scroll(1, 100, 101)
  }

  test("shard1docs100size120") {
    scroll(1, 100, 120)
  }

  test("shard3docs100size3") {
    scroll(3, 100, 3)
  }

  test("shard3docs100size7") {
    scroll(3, 100, 7)
  }

  test("shard3docs100size13") {
    scroll(3, 100, 13)
  }

  test("shard3docs100size24") {
    scroll(3, 100, 24)
  }

  test("shard3docs100size45") {
    scroll(3, 100, 45)
  }

  test("shard3docs100size63") {
    scroll(3, 100, 63)
  }

  test("shard3docs100size89") {
    scroll(3, 100, 89)
  }

  test("shard3docs100size120") {
    scroll(3, 100, 120)
  }

  test("shard3docs100size3Unbalanced") {
    scroll(3, 100, 3, true)
  }

  test("shard3docs100size7Unbalanced") {
    scroll(3, 100, 7, true)
  }

  test("shard3docs100size13Unbalanced") {
    scroll(3, 100, 13, true)
  }

  test("shard3docs100size24Unbalanced") {
    scroll(3, 100, 24, true)
  }

  test("shard3docs100size45Unbalanced") {
    scroll(3, 100, 45, true)
  }

  test("shard3docs100size63Unbalanced") {
    scroll(3, 100, 63, true)
  }

  test("shard3docs100size89Unbalanced") {
    scroll(3, 100, 89, true)
  }

  test("shard3docs100size120Unbalanced") {
    scroll(3, 100, 120)
  }

  private def scroll(numberOfShards: Int, numberOfDocs: Long, size: Int) {
    scroll(numberOfShards, numberOfDocs, size, false)
  }

  private def scroll(numberOfShards: Int, numberOfDocs: Long, size: Int, unbalanced: Boolean) {
    import scala.collection.mutable._
    indexer.createIndex(indexName, settings = Map("number_of_shards" -> numberOfShards.toString, "number_of_replicas" -> "0"))
    indexer.waitForYellowStatus()
    var ids = new HashSet[String]
    var expectedIds = new HashSet[String]
    for (i <- 0 until numberOfDocs.toInt) {
      val id = i.toString
      expectedIds.add(id)
      val routing = if (unbalanced) {
        if (i < (numberOfDocs * 0.6)) "0" else if (i < (numberOfDocs * 0.9)) "1" else "2"
      } else null
      indexer.index(indexName, "type1", id, """{"field": %s}""".format(i), routing = Some(routing))
    }
    indexer.refresh()
    
    var response = indexer.search(searchType = Some(SearchType.SCAN), size = Some(size), scroll = Some("2m"))
    response.hits.totalHits should be === (numberOfDocs)
    var continue = true
    while (continue) {
      response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
      response.hits.totalHits should be === (numberOfDocs)
      response.failedShards() should be === (0)
      for (hit <- response.hits) {
        ids.contains(hit.id()) should be === (false)
        ids.add(hit.id())
      }
      continue = !response.hits.hits.isEmpty
    }
    expectedIds should be === (ids)
  }
}
