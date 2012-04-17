package com.traackr.elasticsearch

import org.elasticsearch.common.settings.ImmutableSettings._
import scala.collection._, JavaConversions._
import scalaz._, Scalaz._

trait IndexCrud extends IndexCreate with IndexDelete with Exists with Aliases
    with Optimize with Flush with Refresh with Status with Stats
    with PutMapping with DeleteMapping {
  self: Indexer =>
}

trait IndexCreate {
  self: Indexer =>

  def createIndex(index: String, settings: Option[String] = None, mappings: Map[String, String] = Map()) = {
    createIndex_send(index, settings, mappings).actionGet
  }

  def createIndex_send(index: String, settings: Option[String] = None, mappings: Map[String, String] = Map()) = {
    createIndex_prepare(index, settings, mappings).execute
  }

  def createIndex_prepare(index: String, settings: Option[String] = None, mappings: Map[String, String] = Map()) = {
    val request = client.admin.indices.prepareCreate(index)
    settings some { that => request.setSettings(settingsBuilder.loadFromSource(that).build()) }
    for ((kind, mapping) <- mappings) request.addMapping(kind, mapping)
    request
  }
}

trait IndexDelete {
  self: Indexer =>
  def deleteIndex(indices: String*) = deleteIndex_send(indices.toArray: _*).actionGet
  def deleteIndex_send(indices: String*) = deleteIndex_prepare(indices.toArray: _*).execute
  def deleteIndex_prepare(indices: String*) = client.admin.indices.prepareDelete(indices.toArray: _*)
}

trait Exists {
  self: Indexer =>
  def exists(indices: String*) = exists_send(indices.toArray: _*).actionGet.exists
  def exists_send(indices: String*) = exists_prepare(indices.toArray: _*).execute
  def exists_prepare(indices: String*) = client.admin.indices.prepareExists(indices.toArray: _*)
}

trait Aliases {
  self: Indexer =>
  def aliases(indices: Seq[String], alias: String, filter: Map[String, Object] = Map()) = aliases_send(indices, alias, filter).actionGet
  def aliases_send(indices: Seq[String], alias: String, filter: Map[String, Object] = Map()) = aliases_prepare(indices, alias, filter).execute
  def aliases_prepare(indices: Seq[String], alias: String, filter: Map[String, Object] = Map()) = {
    val request = client.admin.indices.prepareAliases
    for (each <- indices) request.addAlias(each, alias, filter)
    request
  }
}

trait Optimize {
  self: Indexer =>
  def optimize(indices: String*) = optimize_send(indices.toArray: _*).actionGet
  def optimize_send(indices: String*) = optimize_prepare(indices.toArray: _*).execute
  def optimize_prepare(indices: String*) = client.admin.indices.prepareOptimize(indices.toArray: _*)
}

trait Flush {
  self: Indexer =>
  def flush(indices: String*) = flush_send(indices.toArray: _*).actionGet
  def flush_send(indices: String*) = flush_prepare(indices.toArray: _*).execute
  def flush_prepare(indices: String*) = client.admin.indices.prepareFlush(indices.toArray: _*)
}

trait Refresh {
  self: Indexer =>
  def refresh(indices: String*) = refresh_send(indices.toArray: _*).actionGet
  def refresh_send(indices: String*) = refresh_prepare(indices.toArray: _*).execute
  def refresh_prepare(indices: String*) = client.admin.indices.prepareRefresh(indices.toArray: _*)
}

trait Status {
  self: Indexer =>
  def status(indices: String*) = status_send(indices.toArray: _*).actionGet
  def status_send(indices: String*) = status_prepare(indices.toArray: _*).execute
  def status_prepare(indices: String*) = client.admin.indices.prepareStatus(indices.toArray: _*)
}

trait Stats {
  self: Indexer =>
  def stats(indices: String*) = stats_send(indices.toArray: _*).actionGet
  def stats_send(indices: String*) = stats_prepare(indices.toArray: _*).execute
  def stats_prepare(indices: String*) = client.admin.indices.prepareStats(indices.toArray: _*)
}

trait PutMapping {
  self: Indexer =>
  def putMapping(index: String, `type`: String, json: String) = putMapping_send(index, `type`, json).actionGet
  def putMapping_send(index: String, `type`: String, json: String) = putMapping_prepare(Seq(index), `type`, json).execute
  def putMapping_prepare(indices: Seq[String], `type`: String, json: String) = {
    client.admin.indices
      .preparePutMapping(indices.toArray: _*)
      .setType(`type`)
      .setSource(json)
  }
}

trait DeleteMapping {
  self: Indexer =>
  def deleteMapping(indices: String*) = deleteMapping_send(indices.toArray: _*).actionGet
  def deleteMapping_send(indices: String*) = deleteMapping_prepare(indices.toArray: _*).execute
  def deleteMapping_prepare(indices: String*) = client.admin.indices.prepareDeleteMapping(indices.toArray: _*)
}