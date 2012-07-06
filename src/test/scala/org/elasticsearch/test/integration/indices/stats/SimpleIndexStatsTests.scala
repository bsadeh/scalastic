package org.elasticsearch.test.integration.indices.stats

import org.scalatest._, matchers._
import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.action.admin.indices.stats._
import org.elasticsearch.action.get._
import org.elasticsearch.client._
import org.elasticsearch.test.integration._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SimpleIndexStatsTests extends IndexerBasedTest {

  test("simpleStats") {
    indexer.createIndex("test1")
    indexer.createIndex("test2")
    val clusterHealthResponse = indexer.waitForGreenStatus()
    clusterHealthResponse.timedOut() should be === (false)
    indexer.index("test1", "type1", "1", """{"field": "value"}""")
    indexer.index("test1", "type2", "1", """{"field": "value"}""")
    indexer.index("test2", "type", "1", """{"field": "value"}""")
    indexer.refresh()
    var stats = indexer.stats()
    stats.primaries().docs().count() should be === (3)
    stats.total().docs().count() should be === (6)
    stats.primaries().indexing().total().indexCount() should be === (3)
    stats.total().indexing().total().indexCount() should be === (6)
    stats.total().store() should not be (null)
    stats.total().flush() should be (null)
    stats.index("test1").primaries().docs().count() should be === (2)
    stats.index("test1").total().docs().count() should be === (4)
    stats.index("test1").primaries().store() should not be (null)
    stats.index("test1").primaries().flush() should be (null)
    stats.index("test2").primaries().docs().count() should be === (1)
    stats.index("test2").total().docs().count() should be === (2)
    stats.index("test1").total().indexing().total().indexCurrent() should be === (0)
    stats.index("test1").total().indexing().total().deleteCurrent() should be === (0)
    stats.index("test1").total().search().total().fetchCurrent() should be === (0)
    stats.index("test1").total().search().total().queryCurrent() should be === (0)
    stats = indexer.stats(docs=Some(false), store=Some(false), indexing=Some(false), flush=Some(true), refresh=Some(true), merge=Some(true))
    stats.total().docs() should be (null)
    stats.total().indexing() should be (null)
    stats.total().merge() should not be (null)
    stats.total().flush() should not be (null)
    stats.total().refresh() should not be (null)
     stats = indexer.stats(types=Seq("type1", "type"))
    stats.primaries().indexing().typeStats().get("type1").indexCount() should be === (1)
    stats.primaries().indexing().typeStats().get("type").indexCount() should be === (1)
    stats.primaries().indexing().typeStats().get("type2") should be (null)
    stats.primaries().indexing().typeStats().get("type1").indexCurrent() should be === (0)
    stats.primaries().indexing().typeStats().get("type1").deleteCurrent() should be === (0)
    stats.total().get.count() should be === (0)
    var getResponse = indexer.get("test1", "type1", "1")
    getResponse.exists() should be === (true)
    stats = indexer.stats()
    stats.total().get.count() should be === (1)
    stats.total().get.existsCount() should be === (1)
    stats.total().get.missingCount() should be === (0)
    getResponse = indexer.get("test1", "type1", "2")
    getResponse.exists() should be === (false)
    stats = indexer.stats()
    stats.total().get.count() should be === (2)
    stats.total().get.existsCount() should be === (1)
    stats.total().get.missingCount() should be === (1)
  }
}
