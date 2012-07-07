package org.elasticsearch.test.integration.blocks

import org.scalatest._, matchers._
import org.elasticsearch.common.settings.ImmutableSettings._
import scala.collection._, JavaConversions._
import org.elasticsearch.client._
import org.elasticsearch.cluster.block._
import org.elasticsearch.cluster.metadata._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleBlocksTests extends IndexerBasedTest {

  test("verifyIndexAndClusterReadOnly") {
    val client = indexer.client
    canCreateIndex(client, "test1")
    canIndexDocument(client, "test1")
    setIndexReadOnly(client, "test1", "false")
    canIndexExists(client, "test1")
    setClusterReadOnly(client, "true")
    canNotCreateIndex(client, "test2")
    canNotIndexDocument(client, "test1")
    canNotIndexExists(client, "test1")
    setClusterReadOnly(client, "false")
    canCreateIndex(client, "test2")
    canIndexDocument(client, "test2")
    canIndexDocument(client, "test1")
    canIndexExists(client, "test1")
    canCreateIndex(client, "ro")
    canIndexDocument(client, "ro")
    canIndexExists(client, "ro")
    setIndexReadOnly(client, "ro", "true")
    canNotIndexDocument(client, "ro")
    canNotIndexExists(client, "ro")
    canCreateIndex(client, "rw")
    canIndexDocument(client, "rw")
    canIndexExists(client, "rw")
    setIndexReadOnly(client, "ro", "false")
    canIndexDocument(client, "ro")
    canIndexExists(client, "ro")
  }

  test("testIndexReadWriteMetaDataBlocks") {
    val client = indexer.client
    canCreateIndex(client, "test1")
    canIndexDocument(client, "test1")
    client.admin().indices().prepareUpdateSettings("test1")
      .setSettings(settingsBuilder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true)).execute.actionGet
    canNotIndexDocument(client, "test1")
    client.admin().indices().prepareUpdateSettings("test1")
      .setSettings(settingsBuilder().put(IndexMetaData.SETTING_BLOCKS_WRITE, false)).execute.actionGet
    canIndexDocument(client, "test1")
  }

  private def canCreateIndex(client: Client, index: String) {
    try {
      client.admin().indices().prepareCreate(index).execute.actionGet should not be (null)
    } catch {
      case e: ClusterBlockException => fail
    }
  }

  private def canNotCreateIndex(client: Client, index: String) {
    try {
      client.admin().indices().prepareCreate(index).execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException =>
    }
  }

  private def canIndexDocument(client: Client, index: String) {
    try {
      val builder = client.prepareIndex(index, "zzz")
      builder.setSource("""{"foo": "bar"}""")
      val r = builder.execute.actionGet
      r should not be (null)
    } catch {
      case e: ClusterBlockException => fail
    }
  }

  private def canNotIndexDocument(client: Client, index: String) {
    try {
      val builder = client.prepareIndex(index, "zzz")
      builder.setSource("""{"foo": "bar"}""")
      builder.execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException =>
    }
  }

  private def canIndexExists(client: Client, index: String) {
    try {
      client.admin().indices().prepareExists(index).execute.actionGet should not be (null)
    } catch {
      case e: ClusterBlockException => fail
    }
  }

  private def canNotIndexExists(client: Client, index: String) {
    try {
      client.admin().indices().prepareExists(index).execute.actionGet
      fail
    } catch {
      case e: ClusterBlockException =>
    }
  }

  private def setClusterReadOnly(client: Client, value: String) {
    val newSettings = new mutable.HashMap[String, Any]()
    newSettings.put(MetaData.SETTING_READ_ONLY, value)
    val settingsRequest = client.admin().cluster().prepareUpdateSettings()
    settingsRequest.setTransientSettings(newSettings)
    val settingsResponse = settingsRequest.execute.actionGet
    settingsResponse should not be (null)
  }

  private def setIndexReadOnly(client: Client, index: String, value: AnyRef) {
    val newSettings = new mutable.HashMap[String, Object]()
    newSettings.put(IndexMetaData.SETTING_READ_ONLY, value)
    val settingsRequest = client.admin().indices().prepareUpdateSettings(index)
    settingsRequest.setSettings(newSettings)
    val settingsResponse = settingsRequest.execute.actionGet
    settingsResponse should not be (null)
  }
}
