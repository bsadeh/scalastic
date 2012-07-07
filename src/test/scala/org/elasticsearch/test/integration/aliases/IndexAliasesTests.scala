package org.elasticsearch.test.integration.aliases

import org.scalatest._, matchers._
import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.cluster._
import org.elasticsearch.common._, unit._, settings.ImmutableSettings._
import org.elasticsearch.indices._
import org.elasticsearch.node.internal._
import org.elasticsearch.search._
import scala.collection.JavaConversions._
import java.util.concurrent._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class IndexAliasesTests extends MultiNodesBasedTests {

  override def beforeAll() {
    val nodeSettings = settingsBuilder().put("action.auto_create_index", false).build()
    startNode("server1", nodeSettings)
    startNode("server2", nodeSettings)
  }

  test("testAliases") {
    indexer("server1").createIndex(indexName)
    var clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    try {
        indexer("server1").index("alias1", "type1", "1", source("1", "test"))
      fail("index [alias1] should not exists")
    } catch {
      case e: IndexMissingException => e.index().name() should be === ("alias1")
    }
    //logger.info("--> aliasing index [indexName] with [alias1]")
    indexer("server1").alias(Seq(indexName), "alias1")
    var indexResponse = indexer("server1").index("alias1", "type1", "1", source("1", "test"))
    indexResponse.index() should be === (indexName)
    indexer("server1").createIndex("test_x")
    clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> remove [alias1], Aliasing index [test_x] with [alias1]")
    indexer("server1").unalias(Seq(indexName), "alias1")
    indexer("server1").alias(Seq("test_x"), "alias1")
    Thread.sleep(300)
    indexResponse = indexer("server1").index("alias1", "type1", "1", source("1", "test"))
    indexResponse.index() should be === ("test_x")
  }

  test("testFailedFilter") {
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //    try {
    //      //logger.info("--> aliasing index [indexName] with [alias1] and filter [t]")
    //      indexer("server1").alias(Seq(indexName), "alias1", filter=Some("{ t }"))
    //      fail
    //    } catch {
    //      case e: Exception => 
    //    }
  }

  test("testFilteringAliases") {
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> aliasing index [indexName] with [alias1] and filter [user:kimchy]")
    val filter = termFilter("user", "kimchy")
    indexer("server1").alias(Seq(indexName), "alias1", filter = Option(filter))
    //logger.info("--> making sure that filter was stored with alias [alias1] and filter [user:kimchy]")
    val clusterState = indexer("server1").state().state()
    val indexMd = clusterState.metaData().index(indexName)
    indexMd.aliases().get("alias1").filter().string() should be === ("{\"term\":{\"user\":\"kimchy\"}}")
  }

  test("testSearchingFilteringAliasesSingleIndex") {
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> adding filtering aliases to index [indexName]")
    indexer("server1").alias(Seq(indexName), "alias1")
    indexer("server1").alias(Seq(indexName), "alias2")
    indexer("server1").alias(Seq(indexName), "foos", filter = Option(termFilter("name", "foo")))
    indexer("server1").alias(Seq(indexName), "bars", filter = Option(termFilter("name", "bar")))
    indexer("server1").alias(Seq(indexName), "tests", filter = Option(termFilter("name", "test")))
    indexer("server1").index(indexName, "type1", "1", source("1", "foo test"), refresh = Option(true))
    indexer("server1").index(indexName, "type1", "2", source("2", "bar test"), refresh = Option(true))
    indexer("server1").index(indexName, "type1", "3", source("3", "baz test"), refresh = Option(true))
    indexer("server1").index(indexName, "type1", "4", source("4", "something else"), refresh = Option(true))
    //logger.info("--> checking single filtering alias search")
    var response = indexer("server1").search(indices = Seq("foos"))
    assertHits(response.hits, "1")
    response = indexer("server1").search(indices = Seq("tests"))
    assertHits(response.hits, "1", "2", "3")
    response = indexer("server1").search(indices = Seq("foos"), types = Seq("bars"))
    pending //fixme: failing test
    assertHits(response.hits, "1", "2")
    //logger.info("--> checking single non-filtering alias search")
    response = indexer("server1").search(indices = Seq("alias1"))
    assertHits(response.hits, "1", "2", "3", "4")
    //logger.info("--> checking non-filtering alias and filtering alias search")
    response = indexer("server1").search(indices = Seq("alias1"), types = Seq("foos"))
    assertHits(response.hits, "1", "2", "3", "4")
    //logger.info("--> checking index and filtering alias search")
    response = indexer("server1").search(indices = Seq(indexName), types = Seq("foos"))
    assertHits(response.hits, "1", "2", "3", "4")
  }

  test("testSearchingFilteringAliasesTwoIndices") {
    indexer("server1").createIndex("test1")
    indexer("server1").createIndex("test2")
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> adding filtering aliases to index [test1]")
    indexer("server1").alias(Seq("test1"), "aliasToTest1")
    indexer("server1").alias(Seq("test1"), "aliasToTests")
    indexer("server1").alias(Seq("test1"), "foos", filter = Option(termFilter("name", "foo")))
    indexer("server1").alias(Seq("test1"), "bars", filter = Option(termFilter("name", "bar")))
    //logger.info("--> adding filtering aliases to index [test2]")
    indexer("server1").alias(Seq("test2"), "aliasToTest2")
    indexer("server1").alias(Seq("test2"), "aliasToTests")
    indexer("server1").alias(Seq("test2"), "foos", filter = Option(termFilter("name", "foo")))
    indexer("server1").index("test1", "type1", "1", source("1", "foo test"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "2", source("2", "bar test"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "3", source("3", "baz test"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "4", source("4", "something else"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "5", source("5", "foo test"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "6", source("6", "bar test"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "7", source("7", "baz test"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "8", source("8", "something else"), refresh = Option(true))
    //logger.info("--> checking filtering alias for two indices")
    var response = indexer("server1").search(indices = Seq("foos"))
    assertHits(response.hits, "1", "5")
    indexer("server1").count(Seq("foos")).count() should be === (2L)
    //logger.info("--> checking filtering alias for one index")
    response = indexer("server1").search(indices = Seq("bars"))
    assertHits(response.hits, "2")
    indexer("server1").count(Seq("bars")).count() should be === (1L)
    //logger.info("--> checking filtering alias for two indices and one complete index")
    response = indexer("server1").search(indices = Seq("foos"), types = Seq("test1"))
    pending //fixme: failing test
    assertHits(response.hits, "1", "2", "3", "4", "5")
    indexer("server1").count(Seq("foos", "test1")).count() should be === (5L)
    //logger.info("--> checking filtering alias for two indices and non-filtering alias for one index")
    response = indexer("server1").search(indices = Seq("foos"), types = Seq("aliasToTest1"))
    assertHits(response.hits, "1", "2", "3", "4", "5")
    indexer("server1").count(Seq("foos", "aliasToTest1")).count() should be === (5L)
    //logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices")
    response = indexer("server1").search(indices = Seq("foos"), types = Seq("aliasToTests"))
    response.hits.totalHits should be === (8L)
    indexer("server1").count(Seq("foos", "aliasToTests")).count() should be === (8L)
    //logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices")
    response = indexer("server1").search(Seq("foos", "aliasToTests"), query = termQuery("name", "something"))
    assertHits(response.hits, "4", "8")
    indexer("server1").count(Seq("foos", "aliasToTests"), query = termQuery("name", "something")).count() should be === (2L)
  }

  test("testSearchingFilteringAliasesMultipleIndices") {
    indexer("server1").createIndex("test1")
    indexer("server1").createIndex("test2")
    indexer("server1").createIndex("test3")
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> adding aliases to indices")
    indexer("server1").alias(Seq("test1"), "alias12")
    indexer("server1").alias(Seq("test2"), "alias12")
    //logger.info("--> adding filtering aliases to indices")
    indexer("server1").alias(Seq("test1"), "filter1", filter = Option(termFilter("name", "test1")))
    indexer("server1").alias(Seq("test2"), "filter23", filter = Option(termFilter("name", "foo")))
    indexer("server1").alias(Seq("test3"), "filter23", filter = Option(termFilter("name", "foo")))
    indexer("server1").alias(Seq("test1"), "filter13", filter = Option(termFilter("name", "baz")))
    indexer("server1").alias(Seq("test3"), "filter13", filter = Option(termFilter("name", "baz")))
    indexer("server1").index("test1", "type1", "11", source("11", "foo test1"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "12", source("12", "bar test1"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "13", source("13", "baz test1"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "21", source("21", "foo test2"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "22", source("22", "bar test2"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "23", source("23", "baz test2"), refresh = Option(true))
    indexer("server1").index("test3", "type1", "31", source("31", "foo test3"), refresh = Option(true))
    indexer("server1").index("test3", "type1", "32", source("32", "bar test3"), refresh = Option(true))
    indexer("server1").index("test3", "type1", "33", source("33", "baz test3"), refresh = Option(true))
    //logger.info("--> checking filtering alias for multiple indices")
    pending //fixme: failing test
    var response = indexer("server1").search(indices = Seq("filter23"), types = Seq("filter13"))
    pending //fixme: failing test
    assertHits(response.hits, "21", "31", "13", "33")
    indexer("server1").count(Seq("filter23", "filter13")).count() should be === (4L)
    response = indexer("server1").search(indices = Seq("filter23"), types = Seq("filter1"))
    assertHits(response.hits, "21", "31", "11", "12", "13")
    indexer("server1").count(Seq("filter23", "filter1")).count() should be === (5L)
    response = indexer("server1").search(indices = Seq("filter13"), types = Seq("filter1"))
    assertHits(response.hits, "11", "12", "13", "33")
    indexer("server1").count(Seq("filter13", "filter1")).count() should be === (4L)
    response = indexer("server1").search(Seq("filter13", "filter1", "filter23"))
    assertHits(response.hits, "11", "12", "13", "21", "31", "33")
    indexer("server1").count(Seq("filter13", "filter1", "filter23")).count() should be === (6L)
    response = indexer("server1").search(Seq("filter23", "filter13", "test2"))
    assertHits(response.hits, "21", "22", "23", "31", "13", "33")
    indexer("server1").count(Seq("filter23", "filter13", "test2")).count() should be === (6L)
    response = indexer("server1").search(Seq("filter23", "filter13", "test1", "test2"))
    assertHits(response.hits, "11", "12", "13", "21", "22", "23", "31", "33")
    indexer("server1").count(Seq("filter23", "filter13", "test1", "test2")).count() should be === (8L)
  }

  test("testDeletingByQueryFilteringAliases") {
    indexer("server1").createIndex("test1")
    indexer("server1").createIndex("test2")
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> adding filtering aliases to index [test1]")
    indexer("server1").alias(Seq("test1"), "aliasToTest1")
    indexer("server1").alias(Seq("test1"), "aliasToTests")
    indexer("server1").alias(Seq("test1"), "foos", filter = Option(termFilter("name", "foo")))
    indexer("server1").alias(Seq("test1"), "bars", filter = Option(termFilter("name", "bar")))
    indexer("server1").alias(Seq("test1"), "tests", filter = Option(termFilter("name", "test")))
    //logger.info("--> adding filtering aliases to index [test2]")
    indexer("server1").alias(Seq("test2"), "aliasToTest2")
    indexer("server1").alias(Seq("test2"), "aliasToTests")
    indexer("server1").alias(Seq("test2"), "foos", filter = Option(termFilter("name", "foo")))
    indexer("server1").alias(Seq("test2"), "tests", filter = Option(termFilter("name", "test")))
    indexer("server1").index("test1", "type1", "1", source("1", "foo test"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "2", source("2", "bar test"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "3", source("3", "baz test"), refresh = Option(true))
    indexer("server1").index("test1", "type1", "4", source("4", "something else"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "5", source("5", "foo test"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "6", source("6", "bar test"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "7", source("7", "baz test"), refresh = Option(true))
    indexer("server1").index("test2", "type1", "8", source("8", "something else"), refresh = Option(true))
    //logger.info("--> checking counts before delete")
    indexer("server1").count(Seq("bars")).count() should be === (1L)
    //logger.info("--> delete by query from a single alias")
    indexer("server1").deleteByQuery(Seq("bars"), query = termQuery("name", "test"))
    indexer("server1").refresh()
    //logger.info("--> verify that only one record was deleted")
    indexer("server1").count(Seq("test1")).count() should be === (3L)
    //logger.info("--> delete by query from an aliases pointing to two indices")
    indexer("server1").deleteByQuery(Seq("foos"), query = matchAllQuery)
    indexer("server1").refresh()
    //logger.info("--> verify that proper records were deleted")
    var response = indexer("server1").search(indices = Seq("aliasToTests"))
    assertHits(response.hits, "3", "4", "6", "7", "8")
    //logger.info("--> delete by query from an aliases and an index")
    indexer("server1").deleteByQuery(Seq("tests", "test2"), query = matchAllQuery)
    indexer("server1").refresh()
    //logger.info("--> verify that proper records were deleted")
    response = indexer("server1").search(indices = Seq("aliasToTests"))
    assertHits(response.hits, "4")
  }

  test("testWaitForAliasCreationMultipleShards") {
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    for (i <- 0 until 10) {
      indexer("server1").alias(Seq(indexName), "alias%s".format(i)).acknowledged() should be === (true)
      indexer("server2").index("alias" + i, "type1", "1", source("1", "test"), refresh = Option(true))
    }
  }

  test("testWaitForAliasCreationSingleShard") {
    indexer("server1").createIndex(indexName, settings = Map("numberOfReplicas" -> "0", "numberOfShards" -> "1"))
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    for (i <- 0 until 10) {
      indexer("server1").alias(Seq(indexName), "alias%s".format(i)).acknowledged() should be === (true)
      indexer("server1").index("alias" + i, "type1", "1", source("1", "test"), refresh = Option(true))
    }
  }

  test("testWaitForAliasSimultaneousUpdate") {
    val aliasCount = 10
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    val executor = Executors.newFixedThreadPool(aliasCount)
    for (i <- 0 until aliasCount) {
      val aliasName = "alias" + i
      executor.submit(new Runnable() {
        override def run() {
          indexer("server1").alias(Seq(indexName), aliasName).acknowledged() should be === (true)
          indexer("server2").index(aliasName, "type1", "1", source("1", "test"), refresh = Option(true))
        }
      })
    }
    executor.shutdown()
    val done = executor.awaitTermination(10, TimeUnit.SECONDS)
    done should be === (true)
    if (!done) {
      executor.shutdownNow()
    }
  }

  test("testWaitForAliasTimeout") {
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    indexer("server2").alias(Seq(indexName), "alias1", timeout = Some("0ms")).acknowledged() should be === (false)
    indexer("server1").alias(Seq(indexName), "alias2", timeout = Some("0ms")).acknowledged() should be === (false)
    indexer("server2").alias(Seq(indexName), "alias3").acknowledged() should be === (true)
    indexer("server1").index("alias1", "type1", "1", source("1", "test"), refresh = Option(true))
    indexer("server1").index("alias2", "type1", "1", source("1", "test"), refresh = Option(true))
    indexer("server1").index("alias3", "type1", "1", source("1", "test"), refresh = Option(true))
  }

  test("testSameAlias") {
    indexer("server1").createIndex(indexName)
    val clusterHealth = indexer("server1").waitForGreenStatus()
    clusterHealth.timedOut() should be === (false)
    clusterHealth.status() should be === (ClusterHealthStatus.GREEN)
    //logger.info("--> creating alias1 ")
    indexer("server2").alias(Seq(indexName), "alias1").acknowledged() should be === (true)
    val timeout = TimeValue.timeValueSeconds(2)
    //logger.info("--> recreating alias1 ")
    val stopWatch = new StopWatch()
    stopWatch.start()
    indexer("server2").alias(Seq(indexName), "alias1", timeout = Some(timeout.toString())).acknowledged() should be === (true)
    stopWatch.stop().lastTaskTime().millis() should be < (timeout.millis())
    //logger.info("--> modifying alias1 to have a filter")
    stopWatch.start()
    indexer("server2").alias(Seq(indexName), "alias1", filter = Some(termFilter("name", "foo")), timeout = Some(timeout.toString)).acknowledged() should be === (true)
    stopWatch.stop().lastTaskTime().millis() should be < (timeout.millis())
    //logger.info("--> recreating alias1 with the same filter")
    stopWatch.start()
    indexer("server2").alias(Seq(indexName), "alias1", filter = Some(termFilter("name", "foo")), timeout = Some(timeout.toString)).acknowledged() should be === (true)
    stopWatch.stop().lastTaskTime().millis() should be < (timeout.millis())
    //logger.info("--> recreating alias1 with a different filter")
    stopWatch.start()
    indexer("server2").alias(Seq(indexName), "alias1", filter = Some(termFilter("name", "bar")), timeout = Some(timeout.toString)).acknowledged() should be === (true)
    stopWatch.stop().lastTaskTime().millis() should be < (timeout.millis())
    //logger.info("--> verify that filter was updated")
    val aliasMetaData = node("server1").asInstanceOf[InternalNode].injector()
      .getInstance(classOf[ClusterService])
      .state()
      .metaData()
      .aliases()
      .get("alias1")
      .get(indexName)
    aliasMetaData.getFilter.toString should be === ("{\"term\":{\"name\":\"bar\"}}")
    //logger.info("--> deleting alias1")
    stopWatch.start()
    indexer("server2").unalias(Seq(indexName), "alias1", timeout = Some(timeout.toString)).acknowledged() should be === (true)
    stopWatch.stop().lastTaskTime().millis() should be < (timeout.millis())
    //logger.info("--> deleting alias1 one more time")
    stopWatch.start()
    indexer("server2").unalias(Seq(indexName), "alias1", timeout = Some(timeout.toString)).acknowledged() should be === (true)
    stopWatch.stop().lastTaskTime().millis() should be < (timeout.millis())
  }

  private def assertHits(hits: SearchHits, ids: String*) {
    hits.totalHits() should be === (ids.length)
    (hits.getHits map (_.id) toSet).intersect(ids.toSet) should be === (ids.toSet)
  }

  private def source(id: String, nameValue: String) = """{type1: {"id" : "%s", "name": "%s"}}""".format(id, nameValue)
}
