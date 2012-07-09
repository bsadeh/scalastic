package org.elasticsearch.test.integration.search.basic

import org.scalatest._, matchers._
import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.action.search._
import org.elasticsearch.index.query._
import org.elasticsearch.node._
import com.traackr.scalastic.elasticsearch._

/** This test basically verifies that search with a single shard active (cause we indexed to it) and other
 *  shards possibly not active at all (cause they haven't allocated) will still work.
 */
@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class SearchWhileCreatingIndexTests extends MultiNodesBasedTests {

  test("searchWhileCreatingIndex") {
    val node = startNode("node1")
    for (i <- 0 until 20) {
      node.client().admin().indices().prepareCreate(indexName)
        .setSettings(settingsBuilder().put("number_of_shards", 10)).execute.actionGet
      node.client().prepareIndex(indexName, "type1").setSource("""{"field": "test"}""").execute.actionGet
      node.client().admin().indices().prepareRefresh().execute.actionGet
      val response = node.client().prepareSearch(indexName).setQuery(termQuery("field",
        "test")).execute.actionGet
      response.hits.totalHits should be === (1)
      node.client().admin().indices().prepareDelete(indexName).execute.actionGet
    }
    try {
      node.client().admin().indices().prepareDelete(indexName).execute.actionGet
    } catch {
      case e: Exception =>
    }
  }
}
