package org.elasticsearch.test.integration.indices.mapping

import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class SimpleDeleteMappingTests extends IndexerBasedTest {

  test("simpleDeleteMapping") {
    for (i <- 0 until 10) indexer.index(indexName, "type1", i.toString, """{"value": "test%s"}""".format(i))
    indexer.refresh()
    for (i <- 0 until 10) indexer.count(Nil).getCount should be === (10)

    indexer.state().getState.metaData().index(indexName).mappings().containsKey("type1") should be === (true)
    indexer.deleteMapping(`type` = Some("type1"))
    indexer.refresh()
    for (i <- 0 until 10) indexer.count(Nil).getCount should be === (0)
    indexer.state().getState.metaData().index(indexName).mappings().containsKey("type1") should be === (false)
  }
}
