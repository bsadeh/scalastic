package com.traackr.elasticsearch

trait ClusterAdmin extends Health with Nodes{
  self: Indexer =>
}

trait Nodes {
  self: Indexer =>
  def cluster = client.admin.cluster
  def restartNodes(nodes: String*) = cluster.prepareNodesRestart(nodes.toArray: _*).execute.actionGet
  def shutdownNodes(nodes: String*) = cluster.prepareNodesShutdown(nodes.toArray: _*).execute.actionGet
  def infoForNodes(nodes: String*) = cluster.prepareNodesInfo(nodes.toArray: _*).execute.actionGet
  def statsForNodes(nodes: String*) = cluster.prepareNodesStats(nodes.toArray: _*).execute.actionGet
}

trait Health {
  self: Indexer =>

  def waitForGreenStatus(indices: String*) =
    prepareHealth(indices.toArray: _*).setWaitForGreenStatus.execute.actionGet

  def waitForYellowStatus(indices: String*) =
    prepareHealth(indices.toArray: _*).setWaitForYellowStatus.execute.actionGet

  def waitTillActive(indices: String*) =
    prepareHealth(indices.toArray: _*).setWaitForActiveShards(1).execute.actionGet

  def prepareHealth(indices: String*) =
    client.admin.cluster.prepareHealth(indices.toArray: _*)
}
