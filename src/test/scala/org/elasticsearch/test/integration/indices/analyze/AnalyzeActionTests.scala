package org.elasticsearch.test.integration.indices.analyze

import org.scalatest._, matchers._
import org.elasticsearch.action.admin.indices.analyze._
import org.elasticsearch.client._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class AnalyzeActionTests extends IndexerBasedTest {

  test("simpleAnalyzerTests") {
    for (i <- 0 until 10) {
      val analyzeResponse = indexer.analyze("this is a %s".format(indexName), index = Some(indexName))
      analyzeResponse.tokens().size should be === (1)
      val token = analyzeResponse.tokens().get(0)
      token.term() should be === (indexName)
      token.startOffset() should be === (10)
      token.endOffset() should be === (28)
    }
  }

  test("analyzeWithNoIndex") {
    indexer.deleteIndex()
    var analyzeResponse = indexer.analyze("THIS IS A TEST", analyzer = Some("simple"))
    analyzeResponse.tokens().size should be === (4)
    analyzeResponse = indexer.analyze("THIS IS A TEST", tokenizer = Some("keyword"), tokenFilters = Some("lowercase"))
    analyzeResponse.tokens().size should be === (1)
    analyzeResponse.tokens().get(0).term() should be === ("this is a test")
  }

  test("analyzerWithFieldOrTypeTests") {
    indexer.putMapping(indexName, "document", """{"document": {"properties": {"simple": {"type": "string","analyzer": "simple"}}}}""")
    for (i <- 0 until 10) {
      val analyzeResponse = indexer.analyze("this is a %s".format(indexName), index = Some(indexName), field = Some("document.simple"))
      analyzeResponse.tokens().size should be === (4)
      val token = analyzeResponse.tokens().get(3)
      token.term() should be === (indexName)
      token.startOffset() should be === (10)
      token.endOffset() should be === (28)
    }
  }
}
