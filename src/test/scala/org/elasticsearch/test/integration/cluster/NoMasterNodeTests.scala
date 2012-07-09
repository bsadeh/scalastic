package org.elasticsearch.test.integration.cluster

import org.scalatest._, matchers._
import org.elasticsearch.cluster.block._
import org.elasticsearch.common.unit._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.discovery._
import org.elasticsearch.rest._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class NoMasterNodeTests extends MultiNodesBasedTests {

  override def defaultSettings = Map(
    "discovery.type" -> "zen",
    "action.auto_create_index" -> "false",
    "discovery.zen.minimum_master_nodes" -> "2",
    "discovery.zen.ping_timeout" -> "200ms",
    "discovery.initial_state_timeout" -> "500ms",
    "number_of_shards" -> "1")

  test("testNoMasterActions") {
    val settings = settingsBuilder()
      .put("discovery.type", "zen")
      .put("action.auto_create_index", false)
      .put("discovery.zen.minimum_master_nodes", 2)
      .put("discovery.zen.ping_timeout", "200ms")
      .put("discovery.initial_state_timeout", "500ms")
      .put("index.number_of_shards", 1)
      .build()
    val timeout = TimeValue.timeValueMillis(200)
    val node = startNode("node1", settings)
    val node2 = startNode("node2", settings)
    indexer("node1").createIndex(indexName)
    node2.close()
    Thread.sleep(200)
    val state = node.client().admin().cluster().prepareState().setLocal(true).execute.actionGet
      .state()
    state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK) should be === (true)
    try {
      node.client().prepareGet(indexName, "type1", "1").execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
    }
    try {
      node.client().prepareMultiGet().add(indexName, "type1", "1").execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
    }
    try {
      node.client().preparePercolate(indexName, "type1").setSource("""{}""").execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
    }
    var now = System.currentTimeMillis()
    try {
      node.client().prepareUpdate(indexName, "type1", "1").setScript("test script")
        .setTimeout(timeout).execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => {
        (System.currentTimeMillis() - now) should be > (timeout.millis() - 50)
        e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
      }
    }
    try {
      node.client().admin().indices().prepareAnalyze(indexName, "this is a test").execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
    }
    try {
      node.client().prepareCount(indexName).execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
    }
    now = System.currentTimeMillis()
    try {
      node.client().prepareIndex(indexName, "type1", "1").setSource("""{}""")
        .setTimeout(timeout).execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException => {
        (System.currentTimeMillis() - now) should be > (timeout.millis() - 50)
        e.status() should be === (RestStatus.SERVICE_UNAVAILABLE)
      }
    }
  }
}
