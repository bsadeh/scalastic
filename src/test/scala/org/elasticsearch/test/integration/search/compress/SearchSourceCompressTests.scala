package org.elasticsearch.test.integration.search.compress

import org.scalatest._, matchers._
import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.action.get._
import org.elasticsearch.action.search._
import org.elasticsearch.client._
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.query._
import org.elasticsearch.test.integration._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SearchSourceCompressTests extends IndexerBasedTest {

  test("testSourceFieldCompressed") {
    verifySource(true)
  }

  test("testSourceFieldPlainExplicit") {
    verifySource(false)
  }

  test("testSourceFieldPlain") {
    verifySource(null)
  }

  private def verifySource(compress: java.lang.Boolean) {
    try {
      indexer.deleteIndex(Seq(indexName))
    } catch {
      case e: Exception =>
    }
    indexer.createIndex(indexName)
    indexer.waitForGreenStatus()
    indexer.putMappingForAll(Nil, "type1", """{"type1": {"_source": {"compress": compress}}}""")
    for (i <- 1 until 100) {
      indexer.index(indexName, "type1", i.toString, buildSource(i).string)
    }
    indexer.index(indexName, "type1", "10000", buildSource(10000).string)
    indexer.refresh()
    for (i <- 1 until 100) {
      val getResponse = indexer.get(indexName, "type1", i.toString)
      getResponse.source() should be === (buildSource(i).copiedBytes())
    }
    val getResponse = indexer.get(indexName, "type1", Integer toString 10000)
    getResponse.source() should be === (buildSource(10000).copiedBytes())
    for (i <- 1 until 100) {
      val response = indexer.search(query = idsQuery("type1").ids(i.toString))
      response.hits.getTotalHits should be === (1)
      response.hits.getAt(0).source() should be === (buildSource(i).copiedBytes())
    }
  }

  private def buildSource(count: Int) = {
    val builder = XContentFactory.jsonBuilder().startObject()
    val sb = new StringBuilder()
    for (j <- 0 until count) {
      sb.append("value").append(j).append(' ')
    }
    builder.field("field", sb.toString)
    builder.endObject()
  }
}
