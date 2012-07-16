package org.elasticsearch.test.integration.mlt

import org.elasticsearch.index.query.FilterBuilders._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class MoreLikeThisActionTests extends IndexerBasedTest {

  test("testSimpleMoreLikeThis") {
    indexer.index(indexName, "type1", "1", """{"text": "lucene"}""")
    indexer.index(indexName, "type1", "2", """{"text": "lucene release"}""")
    indexer.refresh()
    val response = indexer.moreLikeThis(indexName, "type1", "1", minTermFreq = Some(1), minDocFreq = Some(1))
    response.successfulShards() should be === (1)
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (1)
  }

  test("testMoreLikeThisWithAliases") {
    indexer.alias(Seq(indexName), "release", filter = Some(termFilter("text", "release")))
    indexer.alias(Seq(indexName), "beta", filter = Some(termFilter("text", "beta")))
    indexer.index(indexName, "type1", "1", """{"text": "lucene beta"}""")
    indexer.index(indexName, "type1", "2", """{"text": "lucene release"}""")
    indexer.index(indexName, "type1", "3", """{"text": "elasticsearch beta"}""")
    indexer.index(indexName, "type1", "4", """{"text": "elasticsearch release"}""")
    indexer.refresh()

    //logger.info("Running moreLikeThis on index")
    var response = indexer.moreLikeThis(indexName, "type1", "1", minTermFreq = Some(1), minDocFreq = Some(1))
    response.hits.totalHits should be === (2)
    //logger.info("Running moreLikeThis on beta shard")
    response = indexer.moreLikeThis("beta", "type1", "1", minTermFreq = Some(1), minDocFreq = Some(1))
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("3")
    //logger.info("Running moreLikeThis on release shard")
    response = indexer.moreLikeThis(indexName, "type1", "1", minTermFreq = Some(1), minDocFreq = Some(1), searchIndices = Seq("release"))
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("2")
  }
}
