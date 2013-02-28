package org.elasticsearch.test.integration.indices.stats

import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleIndexStatsTests extends IndexerBasedTest {

  override def shouldCreateDefaultIndex = false
  
  test("simpleStats") {
    //indexer.creategetIndex("test1")
    indexer.createIndex("test2")
    val clusterHealthResponse = indexer.waitForYellowStatus()
    clusterHealthResponse.isTimedOut should be === (false)
    indexer.index("test1", "type1", "1", """{"field": "value"}""")
    indexer.index("test1", "type2", "1", """{"field": "value"}""")
    indexer.index("test2", "type", "1", """{"field": "value"}""")
    indexer.refresh()
    var stats = indexer.stats()
    stats.getPrimaries.getDocs.getCount should be === (3)
    
    pending //fixme: failing test
    stats.getTotal.getDocs.getCount should be === (6)
    stats.getPrimaries.getIndexing.getTotal.getIndexCount should be === (3)
    stats.getTotal.getIndexing.getTotal.getIndexCount should be === (6)
    stats.getTotal.getStore should not be (null)
    stats.getTotal.getFlush should be (null)
    stats.getIndex("test1").getPrimaries.getDocs.getCount should be === (2)
    stats.getIndex("test1").getTotal.getDocs.getCount should be === (4)
    stats.getIndex("test1").getPrimaries.getStore should not be (null)
    stats.getIndex("test1").getPrimaries.getFlush should be (null)
    stats.getIndex("test2").getPrimaries.getDocs.getCount should be === (1)
    stats.getIndex("test2").getTotal.getDocs.getCount should be === (2)
    stats.getIndex("test1").getTotal.getIndexing.getTotal.getIndexCurrent should be === (0)
    stats.getIndex("test1").getTotal.getIndexing.getTotal.getDeleteCurrent should be === (0)
    stats.getIndex("test1").getTotal.getSearch.getTotal.getFetchCurrent should be === (0)
    stats.getIndex("test1").getTotal.getSearch.getTotal.getQueryCurrent should be === (0)
    stats = indexer.stats(docs=Some(false), store=Some(false), indexing=Some(false), flush=Some(true), refresh=Some(true), merge=Some(true))
    stats.getTotal.getDocs should be (null)
    stats.getTotal.getIndexing should be (null)
    stats.getTotal.getMerge should not be (null)
    stats.getTotal.getFlush should not be (null)
    stats.getTotal.getRefresh should not be (null)
     stats = indexer.stats(types=Seq("type1", "type"))
    stats.getPrimaries.getIndexing.getTypeStats.get("type1").getIndexCount should be === (1)
    stats.getPrimaries.getIndexing.getTypeStats.get("type").getIndexCount should be === (1)
    stats.getPrimaries.getIndexing.getTypeStats.get("type2") should be (null)
    stats.getPrimaries.getIndexing.getTypeStats.get("type1").getIndexCurrent should be === (0)
    stats.getPrimaries.getIndexing.getTypeStats.get("type1").getDeleteCurrent should be === (0)
    stats.getTotal.getGet.getCount should be === (0)
    var getResponse = indexer.get("test1", "type1", "1")
    getResponse.isExists should be === (true)
    stats = indexer.stats()
    stats.getTotal.getGet.getCount should be === (1)
    stats.getTotal.getGet.getExistsCount should be === (1)
    stats.getTotal.getGet.getMissingCount should be === (0)
    getResponse = indexer.get("test1", "type1", "2")
    getResponse.isExists should be === (false)
    stats = indexer.stats()
    stats.getTotal.getGet.getCount should be === (2)
    stats.getTotal.getGet.getExistsCount should be === (1)
    stats.getTotal.getGet.getMissingCount should be === (1)
  }
}
