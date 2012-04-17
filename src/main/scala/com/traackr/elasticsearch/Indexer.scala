package com.traackr.elasticsearch

import org.elasticsearch.client._, transport._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.node._, NodeBuilder._
import scala.collection.JavaConversions._

object Indexer {
  import org.elasticsearch.common.transport._

  def transport(settings: Map[String, String] = Map(), host: String = "localhost", ports: Seq[Int] = Seq(9300)) = {
    val builder = settingsBuilder
    for ((key, value) <- settings) builder.put(key, value)
    val client = new TransportClient(builder.build)
    for (each <- ports) client.addTransportAddress(new InetSocketTransportAddress(host, each))
    new ClientIndexer(client)
  }

  def using(settings: String): Indexer = using(settingsBuilder.loadFromSource(settings))

  def using(settings: Map[String, String]): Indexer = using(settingsBuilder.put(settings))

  private def using(builder: Builder) = at(nodeBuilder.settings(builder).build)

  def local = at(nodeBuilder.local(true).data(true).node)

  def at(node: Node) = new NodeIndexer(node)
}

trait Indexer extends ClusterAdmin with IndexCrud with Analysis with Indexing with Searching {
  val client: Client
  def start: Indexer
  def stop

  def catchUpOn(`type`: String, bar: Int, seed: Int = 1) = catchUpOnAll(Seq(`type`), bar, seed)
  def catchUpOnAll(types: Seq[String] = Seq(), bar: Int, seed: Int = 1) = {
    var factor = seed
    while (count(types = types) < bar) {
      println("catching up in %s sec ...".format(factor))
      Thread sleep factor * 1000
      factor *= 2
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

