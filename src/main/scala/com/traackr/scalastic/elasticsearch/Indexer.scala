package com.traackr.scalastic.elasticsearch

import org.elasticsearch.client._, transport._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.node._, NodeBuilder._
import scala.collection.JavaConversions._
import org.slf4j._

object Indexer {
  import org.elasticsearch.common.transport._

  def transport(settings: Map[String, String], host: String = "localhost", ports: Seq[Int] = Seq(9300)) = {
    require(settings.contains("cluster.name"))
    val builder = settingsBuilder
    for ((key, value) <- settings) builder.put(key, value)
    builder.put("client.transport.sniff", true)
    val client = new TransportClient(builder)
    for (each <- ports) client.addTransportAddress(new InetSocketTransportAddress(host, each))
    new ClientIndexer(client)
  }

  def using(settings: String): Indexer = using(settingsBuilder.loadFromSource(settings))

  def using(settings: Map[String, String]): Indexer = using(settingsBuilder.put(settings))

  private def using(builder: Builder) = at(nodeBuilder.settings(builder).build)

  def local = at(nodeBuilder.local(true).data(true).node)

  def at(node: Node) = new NodeIndexer(node)
}


trait Indexer extends Logging with ClusterAdmin with IndexCrud with Analysis with Indexing with Searching with WaitingForGodot {
  val client: Client
  def start: Indexer
  def stop

  /** reindexing using a supplied <reindexing> function
   *  the reindexing has sole control over how to retrieve the data to be indexed in <targetIndex>.
   *  it can retrieve it from an external data source, from <sourceIndex>, etc.
   *  however, don't forget to consider where the function is invoked (here), which might be
   *  very different from where it was called (as in different module and/or jvm ).
   */
  def reindexWith[A](sourceIndex: String, targetIndex: String)(reindexing: (Indexer, String) => A) = {
    // before whole data indexing do:
    //	- record the sourceIndex settings ...
    val sourceSettings = metadataFor(sourceIndex).settings
    //	- ... then create the new targetIndex, without replication first (for faster indexing)
    val withoutReplicas = sourceSettings.getAsMap + ("index.number_of_replicas" -> "0")
    createIndex(index = targetIndex, settings = withoutReplicas.toMap)
    waitTillActive()
    try {
      // invoking the function that will create a new targetIndex from scratch
      reindexing(this, targetIndex)
    } finally {
      // after whole data indexing do:
      //	- optimize the newly indexed ...
      optimize(targetIndex)
      // 	- update targetIndex with sourceIndex settings ...
      updateSettings("""{"number_of_replicas": %s}""".format(sourceSettings.get("index.number_of_replicas")), targetIndex)
      //	- ... then transfer aliases from sourceIndex to targetIndex
      for (each <- metadataFor(sourceIndex).aliases.values) {
        unalias(each.alias, sourceIndex)
        alias(each.alias, targetIndex)
      }
    }
  }
}

private[elasticsearch] class NodeIndexer(node: Node) extends Indexer {
  val client = node.client
  def start(): Indexer = { node.start; waitForYellowStatus(); this }
  def stop() = node.close
}

private[elasticsearch] class ClientIndexer(val client: Client) extends Indexer {
  def start(): Indexer = { waitForYellowStatus(); this }
  def stop() = client.close
}

