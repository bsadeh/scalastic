package com.traackr.elasticsearch

import org.scalatest._, matchers._
import org.elasticsearch.action.search._
import org.elasticsearch.common.logging._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.index.query._, QueryBuilders._
import com.traackr.util.Conversions._

abstract class IndexerBasedTest extends FunSuite with ShouldMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll with OneInstancePerTest with UsingIndexer {

  val logger = Loggers.getLogger(getClass())

  override def beforeAll {
    indexer_beforeAll
  }

  override def beforeEach {
    indexer_beforeEach
  }

  override def afterAll {
    indexer_afterAll
  }

  def createDefaultIndex() {
    indexer.createIndex(index = indexName, settings = """{"number_of_shards":1}""")
    indexer.waitTillActive()
  }

  def search(query: String): SearchResponse = {
    search(queryString(query))
  }

  def search(queryBuilder: QueryBuilder): SearchResponse = {
    indexer.search(query = queryBuilder)
  }

  def shouldHaveNoFailures(response: SearchResponse) = {
    response.shardFailures().length should be === 0
    response.failedShards() should be === 0
  }

  def valueFor[A](response: SearchResponse, whichHit: Int, field: String): A = {
    response.hits.getAt(whichHit).fields.get(field).value()
  }

  def print(response: SearchResponse) = {
    response.hits.getHits foreach { hit =>
      println("======= %s =======\n%s".format(hit.getType, hit.getSource.mkString(", ")))
    }
    println("------------------------------------")
  }

}