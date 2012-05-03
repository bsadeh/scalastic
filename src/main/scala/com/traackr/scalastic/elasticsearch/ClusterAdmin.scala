package com.traackr.scalastic.elasticsearch

trait ClusterAdmin extends Health with Nodes with State with Metadata {
  self: Indexer =>
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

trait Nodes {
  self: Indexer =>
  def cluster = client.admin.cluster
  def restartNodes(nodes: String*) = cluster.prepareNodesRestart(nodes.toArray: _*).execute.actionGet
  def shutdownNodes(nodes: String*) = cluster.prepareNodesShutdown(nodes.toArray: _*).execute.actionGet
  def infoForNodes(nodes: String*) = cluster.prepareNodesInfo(nodes.toArray: _*).execute.actionGet
  def statsForNodes(nodes: String*) = cluster.prepareNodesStats(nodes.toArray: _*).execute.actionGet
}

trait State {
  self: Indexer =>
  def state = state_send.actionGet
  def state_send = state_prepare.execute
  def state_prepare = client.admin.cluster.prepareState
}

trait Metadata {
  self: State =>
  def metadata = state.state.metaData
  def metadataFor(index: String) = metadata.index(index)
  def metadataFor(index: String, `type`: String) = metadata.index(index).mappings.get(`type`)
  def fieldsOf(index: String, `type`: String) = {
    metadataFor(index, `type`).sourceAsMap.get("properties").asInstanceOf[Map[String, Object]]
  }
}
