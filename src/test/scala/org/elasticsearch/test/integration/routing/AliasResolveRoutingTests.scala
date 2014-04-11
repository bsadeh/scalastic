package org.elasticsearch.test.integration.routing

import org.elasticsearch.cluster.metadata.AliasAction._
import org.elasticsearch._
import org.elasticsearch.client._
import org.elasticsearch.cluster._
import org.elasticsearch.node.internal._
import scala.collection._, JavaConversions._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) class AliasResolveRoutingTests extends IndexerBasedTest {

  override def shouldCreateDefaultIndex = false

  test("testResolveIndexRouting") {
    indexer.createIndex("test1")
    indexer.createIndex("test2")
    indexer.waitForYellowStatus()
    indexer.alias(Seq("test1"), "alias", actions = Seq(
      newAddAliasAction("test1", "alias10").routing("0"),
      newAddAliasAction("test1", "alias110").searchRouting("1,0"),
      newAddAliasAction("test1", "alias12").routing("2"),
      newAddAliasAction("test2", "alias20").routing("0"),
      newAddAliasAction("test2", "alias21").routing("1"),
      newAddAliasAction("test1", "alias0").routing("0"),
      newAddAliasAction("test2", "alias0").routing("0")))
    indexer.metadata.resolveIndexRouting(null, "test1") should be(null)
    indexer.metadata.resolveIndexRouting(null, "test1") should be(null)
    indexer.metadata.resolveIndexRouting(null, "alias10") should equal ("0")
    indexer.metadata.resolveIndexRouting(null, "alias20") should equal ("0")
    indexer.metadata.resolveIndexRouting(null, "alias21") should equal ("1")
    indexer.metadata.resolveIndexRouting("3", "test1") should equal ("3")
    indexer.metadata.resolveIndexRouting("0", "alias10") should equal ("0")
    try {
      indexer.metadata.resolveIndexRouting("1", "alias10")
      fail
    } catch {
      case e: ElasticsearchIllegalArgumentException =>
    }
    try {
      indexer.metadata.resolveIndexRouting(null, "alias0")
      fail
    } catch {
      case ex: ElasticsearchIllegalArgumentException =>
    }
  }

  test("testResolveSearchRouting") {
    indexer.createIndex("test1")
    indexer.createIndex("test2")
    indexer.waitForYellowStatus()
    indexer.alias(Seq("test1"), "alias", actions = Seq(
      newAddAliasAction("test1", "alias10").routing("0"),
      newAddAliasAction("test2", "alias20").routing("0"),
      newAddAliasAction("test2", "alias21").routing("1"),
      newAddAliasAction("test1", "alias0").routing("0"),
      newAddAliasAction("test2", "alias0").routing("0")))
    indexer.metadata.resolveSearchRouting(null, "alias") should be(null)
    indexer.metadata.resolveSearchRouting("0,1", "alias") should equal (JMap("test1" -> JSet("0", "1")))
    indexer.metadata.resolveSearchRouting(null, "alias10") should equal (JMap("test1" -> JSet("0")))
    indexer.metadata.resolveSearchRouting(null, "alias10") should equal (JMap("test1" -> JSet("0")))
    indexer.metadata.resolveSearchRouting("0", "alias10") should equal (JMap("test1" -> JSet("0")))
    indexer.metadata.resolveSearchRouting("1", "alias10") should be(null)
    indexer.metadata.resolveSearchRouting(null, "alias0") should equal (JMap("test1" -> JSet("0"), "test2" -> JSet("0")))
    indexer.metadata.resolveSearchRouting(null, Array("alias10", "alias20")) should equal (JMap("test1" -> JSet("0"), "test2" -> JSet("0")))
    indexer.metadata.resolveSearchRouting(null, Array("alias10", "alias21")) should equal (JMap("test1" -> JSet("0"), "test2" -> JSet("1")))
    indexer.metadata.resolveSearchRouting(null, Array("alias20", "alias21")) should equal (JMap("test2" -> JSet("0", "1")))
    indexer.metadata.resolveSearchRouting(null, Array("test1", "alias10")) should be(null)
    indexer.metadata.resolveSearchRouting(null, Array("alias10", "test1")) should be(null)
    indexer.metadata.resolveSearchRouting("0", Array("alias10", "alias20")) should equal (JMap("test1" -> JSet("0"), "test2" -> JSet("0")))
    indexer.metadata.resolveSearchRouting("0,1", Array("alias10", "alias20")) should equal (JMap("test1" -> JSet("0"), "test2" -> JSet("0")))
    indexer.metadata.resolveSearchRouting("1", Array("alias10", "alias20")) should be(null)
    indexer.metadata.resolveSearchRouting("0", Array("alias10", "alias21")) should equal (JMap("test1" -> JSet("0")))
    indexer.metadata.resolveSearchRouting("1", Array("alias10", "alias21")) should equal (JMap("test2" -> JSet("1")))
    indexer.metadata.resolveSearchRouting("0,1,2", Array("alias10", "alias21")) should equal (JMap("test1" -> JSet("0"), "test2" -> JSet("1")))
    indexer.metadata.resolveSearchRouting("0,1,2", Array("test1", "alias10", "alias21")) should equal (JMap("test1" -> JSet("0", "1", "2"), "test2" -> JSet("1")))
  }

  private def JSet[A](elements: A*) = new java.util.HashSet(elements)

  private def JMap[A, B](pairs: Tuple2[A, B]*) = {
    val result = new java.util.HashMap[A, B]
    pairs foreach { case (k, v) => result.put(k, v) }
    result
  }
}
