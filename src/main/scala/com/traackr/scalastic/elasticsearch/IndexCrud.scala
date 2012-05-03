package com.traackr.scalastic.elasticsearch

import org.elasticsearch.common.settings.ImmutableSettings._
import scala.collection._, JavaConversions._

trait IndexCrud extends IndexCreate with IndexDelete with UpdateSettings with Exists with Aliases
    with Optimize with Flush with Refresh with Status with Stats
    with PutMapping with DeleteMapping {
  self: Indexer =>
}

trait IndexCreate {
  self: Indexer =>

  def createIndex(index: String, settings: String = "", mappings: Map[String, String] = Map()) = {
    createIndex_send(index, settings, mappings).actionGet
  }

  def createIndex_send(index: String, settings: String = "", mappings: Map[String, String] = Map()) = {
    createIndex_prepare(index, settings, mappings).execute
  }

  def createIndex_prepare(index: String, settings: String = "", mappings: Map[String, String] = Map()) = {
    val request = client.admin.indices.prepareCreate(index)
    if (!settings.isEmpty) request.setSettings(settingsBuilder.loadFromSource(settings).build())
    for ((kind, mapping) <- mappings) request.addMapping(kind, mapping)
    request
  }
}

trait IndexDelete {
  self: Indexer =>
  def deleteIndex(indices: String*) = deleteIndex_send(indices.toArray: _*).actionGet
  def deleteIndex_send(indices: String*) = deleteIndex_prepare(indices.toArray: _*).execute
  def deleteIndex_prepare(indices: String*) = client.admin.indices.prepareDelete(indices.toArray: _*)

  def deleteIndexIfExists(indices: String*) = deleteIndexIfExists_send(indices.toArray: _*).actionGet
  def deleteIndexIfExists_send(indices: String*) = deleteIndexIfExists_prepare(indices.toArray: _*).execute
  def deleteIndexIfExists_prepare(indices: String*) = client.admin.indices.prepareDelete(indices filter (exists(_)) toArray: _*)
}

trait UpdateSettings {
  self: Indexer =>
  def updateSettings(settings: String, indices: String*) = updateSettings_send(settings, indices.toArray: _*).actionGet
  def updateSettings_send(settings: String, indices: String*) = updateSettings_prepare(settings, indices.toArray: _*).execute
  def updateSettings_prepare(settings: String, indices: String*) = {
    val request = client.admin.indices.prepareUpdateSettings(indices.toArray: _*)
    request.setSettings(settingsBuilder.loadFromSource(settings).build())
    request
  }
}

trait Exists {
  self: Indexer =>
  def exists(indices: String*) = exists_send(indices.toArray: _*).actionGet.exists
  def exists_send(indices: String*) = exists_prepare(indices.toArray: _*).execute
  def exists_prepare(indices: String*) = client.admin.indices.prepareExists(indices.toArray: _*)
}

trait Aliases {
  self: Indexer =>

  def alias(alias: String, indices: String*) = alias_send(alias, indices.toArray: _*).actionGet
  def alias_send(alias: String, indices: String*) = alias_prepare(alias, indices.toArray: _*).execute
  def alias_prepare(alias: String, indices: String*) = {
    val request = client.admin.indices.prepareAliases
    for (each <- indices) request.addAlias(each, alias)
    request
  }

  def alias(alias: String, filter: Map[String, Object], indices: String*) = alias_send(alias, filter, indices.toArray: _*).actionGet
  def alias_send(alias: String, filter: Map[String, Object], indices: String*) = alias_prepare(alias, filter, indices.toArray: _*).execute
  def alias_prepare(alias: String, filter: Map[String, Object], indices: String*) = {
    val request = client.admin.indices.prepareAliases
    for (each <- indices) request.addAlias(each, alias, filter)
    request
  }

  def unalias(alias: String, indices: String*) = unalias_send(alias, indices.toArray: _*).actionGet
  def unalias_send(alias: String, indices: String*) = unalias_prepare(alias, indices.toArray: _*).execute
  def unalias_prepare(alias: String, indices: String*) = {
    val request = client.admin.indices.prepareAliases
    for (each <- indices) request.removeAlias(each, alias)
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
  def putMapping_prepare(indices: Iterable[String], `type`: String, json: String) = {
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