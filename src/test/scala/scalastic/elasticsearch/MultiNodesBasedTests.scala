package scalastic.elasticsearch

import org.scalatest._, matchers._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.common.network._
import org.elasticsearch.common.settings._
import org.elasticsearch.node._
import scala.collection._, JavaConversions._
import org.elasticsearch.Conversions._

import scalastic.elasticsearch._

abstract class MultiNodesBasedTests extends FunSuiteLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  def indexName = getClass.getSimpleName.toLowerCase

  def defaultSettings = Map("cluster.name" -> "test-cluster-%s".format(NetworkUtils.getLocalAddress.getHostName))

  private val indexers = new mutable.HashMap[String, Indexer]
  def indexer(id: String): Indexer = indexers.getOrElse(id, null)
  def node(id: String): Node = indexer(id).asInstanceOf[NodeIndexer].node

  override def beforeEach = indexers.values foreach (_.deleteIndex(Set("_all")))

  override def afterAll = closeAllNodes
  def closeAllNodes() = {
    indexers.values foreach (_.stop)
    indexers.clear
  }
  def closeNode(id: String) = indexers.remove(id) foreach (_.stop)

  def startNode(id: String): Node = buildNode(id).start()
  def startNode(id: String, settings: Settings.Builder): Node = startNode(id, settings.build())
  def startNode(id: String, settings: Settings): Node = buildNode(id, settings).start()

  def buildNode(id: String): Node = buildNode(id, Builder.EMPTY_SETTINGS)
  def buildNode(id: String, settings: Settings.Builder): Node = buildNode(id, settings.build())
  def buildNode(id: String, settings: Settings): Node = {
    val settingsSource = getClass.getName.replace('.', '/') + ".yml"
    var finalSettings = settingsBuilder.loadFromClasspath(settingsSource)
      .put(defaultSettings.toSettings)
      .put(settings)
      .put("name", id)
      .build()
    if (finalSettings.get("gateway.type") == null)
      finalSettings = settingsBuilder.put(finalSettings).put("gateway.type", "none").build()
    if (finalSettings.get("cluster.routing.schedule") != null)
      finalSettings = settingsBuilder.put(finalSettings).put("cluster.routing.schedule", "50ms").build()
    val node = nodeBuilder().settings(finalSettings).build()
    indexers.put(id, Indexer.at(node))
    node
  }
}
