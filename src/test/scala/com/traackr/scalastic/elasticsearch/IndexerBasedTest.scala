package com.traackr.scalastic.elasticsearch

import org.scalatest._, matchers._
import org.elasticsearch.action.search._
import org.elasticsearch.index.query._, QueryBuilders._
import scala.collection.JavaConversions._

abstract class IndexerBasedTest extends FunSuite with ShouldMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll with UsingIndexer {
//  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def beforeAll = indexer_beforeAll

  override def afterAll = indexer_afterAll

  override def beforeEach {
	  indexer_beforeEach
	  indexer.count().count should be === 0
	  if (shouldCreateDefaultIndex) createDefaultIndex
  }
  
  def shouldCreateDefaultIndex = true

  def indexSettings = Map("number_of_shards" -> "1")

  def createDefaultIndex() {
    indexer.createIndex(index = indexName, settings = indexSettings)
    indexer.waitTillActive()
  }

  def search(query: String) = indexer.search(query = queryString(query))

  def search(queryBuilder: QueryBuilder) = indexer.search(query = queryBuilder)

  def catchUpOn(`type`: String, howMany: Int) = indexer.waitTillCountAtLeast(Seq(indexName), `type`, howMany)

  def shouldHaveNoFailures(response: SearchResponse) = {
    response.shardFailures().length should be === 0
    response.failedShards() should be === 0
  }

  def valueFor[A](response: SearchResponse, whichHit: Int, field: String): A = {
    response.hits.getAt(whichHit).fields.get(field).value()
  }

}