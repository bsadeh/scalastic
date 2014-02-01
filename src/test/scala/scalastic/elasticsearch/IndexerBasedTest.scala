package scalastic.elasticsearch

import org.scalatest._, matchers._
import org.elasticsearch.action.search._
import org.elasticsearch.index.query._, QueryBuilders._

abstract class IndexerBasedTest extends FunSuite with ShouldMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll with UsingIndexer {
//  val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def beforeAll() { indexer_beforeAll }

  override def afterAll() { indexer_afterAll }

  override def beforeEach() {
	  indexer_beforeEach
	  indexer.count().getCount should equal (0)
	  if (shouldCreateDefaultIndex) createDefaultIndex()
  }
  
  def shouldCreateDefaultIndex = true

  def defaultSettings = Map("number_of_shards" -> "1")

  def createDefaultIndex() {
    indexer.createIndex(index = indexName, settings = defaultSettings)
    indexer.waitTillActive()
  }

  def search(query: String) = indexer.search(query = queryString(query))

  def search(queryBuilder: QueryBuilder) = indexer.search(query = queryBuilder)

  def catchUpOn(`type`: String, howMany: Int) {
    indexer.waitTillCountAtLeast(Seq(indexName), `type`, howMany)
  }

  def shouldHaveNoFailures(response: SearchResponse) {
    response.getShardFailures.length should equal (0)
    response.getFailedShards should equal (0)
  }

  def valueFor[A](response: SearchResponse, whichHit: Int, field: String): A = {
    response.getHits.getAt(whichHit).fields.get(field).value()
  }

}