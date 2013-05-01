package org.elasticsearch.test.integration.indices.analyze

import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class AnalyzeActionTests extends IndexerBasedTest {

  test("simpleAnalyzerTests") {
    for (i <- 0 until 10) {
      val analyzeResponse = indexer.analyze("this is a %s".format(indexName), index = Some(indexName))
      analyzeResponse.getTokens.size should be === (1)
      val token = analyzeResponse.getTokens.get(0)
      token.getTerm should be === (indexName)
      token.getStartOffset should be === (10)
      token.getEndOffset should be === (28)
    }
  }

  test("analyzeWithNoIndex") {
    indexer.deleteIndex()
    var analyzeResponse = indexer.analyze("THIS IS A TEST", analyzer = Some("simple"))
    analyzeResponse.getTokens.size should be === (4)
    analyzeResponse = indexer.analyze("THIS IS A TEST", tokenizer = Some("keyword"), tokenFilters = Some("lowercase"))
    analyzeResponse.getTokens.size should be === (1)
    analyzeResponse.getTokens.get(0).getTerm should be === ("this is a test")
  }

  test("analyzerWithFieldOrTypeTests") {
    indexer.putMapping(indexName, "document", """{"document": {"properties": {"simple": {"type": "string","analyzer": "simple"}}}}""")
    for (i <- 0 until 10) {
      val analyzeResponse = indexer.analyze("this is a %s".format(indexName), index = Some(indexName), field = Some("document.simple"))
      analyzeResponse.getTokens.size should be === (4)
      val token = analyzeResponse.getTokens.get(3)
      token.getTerm should be === (indexName)
      token.getStartOffset should be === (10)
      token.getEndOffset should be === (28)
    }
  }
}
