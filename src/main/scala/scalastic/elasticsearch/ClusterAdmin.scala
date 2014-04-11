package scalastic.elasticsearch

trait ClusterAdmin extends Health
  with Nodes
  with State
  with Metadata
  with ClusterStats {
  self: Indexer =>
}

trait Health {
  self: Indexer =>

  import org.elasticsearch.action.admin.cluster.health._, ClusterHealthStatus._

  def waitForGreenStatus(indices: Iterable[String] = Nil) =
    waitForStatus(indices, GREEN)

  def waitForYellowStatus(indices: Iterable[String] = Nil) =
    waitForStatus(indices, YELLOW)

  def waitForStatus(indices: Iterable[String] = Nil, status: ClusterHealthStatus, timeout: Option[String] = None) =
    health_prepare(indices, timeout).setWaitForStatus(status).execute.actionGet

  def waitTillActive(indices: Iterable[String] = Nil, howMany: Int = 1) =
    health_prepare(indices).setWaitForActiveShards(howMany).execute.actionGet

  def waitForRelocating(indices: Iterable[String] = Nil, howMany: Int = 1) =
    health_prepare(indices).setWaitForRelocatingShards(howMany).execute.actionGet

  def waitForNodes(indices: Iterable[String] = Nil, howMany: String = ">0") =
    health_prepare(indices).setWaitForNodes(howMany).execute.actionGet

  def health_prepare(indices: Iterable[String] = Nil, timeout: Option[String] = None) = {
    val request = client.admin.cluster.prepareHealth(indices.toArray: _*)
    timeout foreach { request.setTimeout(_) }
    request
  }
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
  def state(
    filterBlocks: Option[Boolean] = None,
    filterMetaData: Option[Boolean] = None,
    filter: Option[Boolean] = None,
    filterIndexTemplates: Iterable[String] = Nil,
    filterIndices: Iterable[String] = Nil,
    filterNodes: Option[Boolean] = None,
    filterRoutingTable: Option[Boolean] = None,
    local: Option[Boolean] = None,
    timeout: Option[String] = None) = state_send(filterBlocks, filterMetaData, filter, filterIndexTemplates, filterIndices, filterNodes, filterRoutingTable, local, timeout).actionGet

  def state_send(
    filterBlocks: Option[Boolean] = None,
    filterMetaData: Option[Boolean] = None,
    filter: Option[Boolean] = None,
    filterIndexTemplates: Iterable[String] = Nil,
    filterIndices: Iterable[String] = Nil,
    filterNodes: Option[Boolean] = None,
    filterRoutingTable: Option[Boolean] = None,
    local: Option[Boolean] = None,
    timeout: Option[String] = None) = state_prepare(filterBlocks, filterMetaData, filter, filterIndexTemplates, filterIndices, filterNodes, filterRoutingTable, local, timeout).execute

  def state_prepare(
    filterBlocks: Option[Boolean] = None,
    filterMetaData: Option[Boolean] = None,
    filter: Option[Boolean] = None,
    filterIndexTemplates: Iterable[String] = Nil,
    filterIndices: Iterable[String] = Nil,
    filterNodes: Option[Boolean] = None,
    filterRoutingTable: Option[Boolean] = None,
    local: Option[Boolean] = None,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.admin.cluster.prepareState
    filterBlocks foreach { request.setBlocks(_) }
    request.setIndexTemplates(filterIndexTemplates.toArray: _*)
    request.setIndices(filterIndices.toArray: _*)
    filterMetaData foreach { request.setMetaData(_) }
    filterNodes foreach { request.setNodes(_) }
    filterRoutingTable foreach { request.setRoutingTable(_) }
    local foreach { request.setLocal(_) }
    timeout foreach { request.setMasterNodeTimeout(_) }
    request
  }
}

trait Metadata {
  self: State =>
  def metadata = state().getState.metaData
  def metadataFor(index: String) = metadata.index(index)
  def metadataFor(index: String, `type`: String) = metadata.index(index).mappings.get(`type`)
  def fieldsOf(index: String, `type`: String) = metadataFor(index, `type`).sourceAsMap.get("properties").asInstanceOf[Map[String, Object]]
}

trait ClusterStats {
  self: Indexer =>

  def clusterStats() = clusterStats_send().actionGet
  def clusterStats_send() = clusterStats_prepare().execute
  def clusterStats_prepare() = client.admin.cluster.prepareClusterStats
}
