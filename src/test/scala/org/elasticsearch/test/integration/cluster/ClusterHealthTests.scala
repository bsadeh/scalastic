package org.elasticsearch.test.integration.cluster

import org.elasticsearch.action.admin.cluster.health._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ClusterHealthTests extends MultiNodesBasedTests {

  test("testHealth") {
    val node1 = startNode("node1")
    //logger.info("--> running cluster health on an index that does not exists")
    val healthResponse = node1.client().admin().cluster().prepareHealth(indexName)
      .setWaitForYellowStatus()
      .setTimeout("1s").execute.actionGet
    healthResponse.isTimedOut should be === (true)
    healthResponse.getStatus should be === (ClusterHealthStatus.RED)
  }
}
