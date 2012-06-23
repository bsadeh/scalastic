package com.traackr.scalastic.elasticsearch

import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.common.unit._, TimeValue._
import scala.collection._, JavaConversions._

/*
fixme: add these api traits:

prepareOpen(String)
prepareClose(String)

preparePutTemplate(String)
prepareDeleteTemplate(String)

prepareClearCache(String...)
prepareGatewaySnapshot(String...)
prepareSegments(String...)
prepareValidateQuery(String...)
 */
trait IndexCrud
    extends IndexCreate
    with IndexDelete
    with UpdateSettings
    with Exists
    with Alias
    with Unalias
    with Optimize
    with Flush
    with Refresh
    with Status
    with Stats
    with PutMapping
    with DeleteMapping {
  self: Indexer =>
}

trait IndexCreate {
  self: Indexer =>

  def createIndex(index: String, settings: Map[String, String] = Map(), mappings: Map[String, String] = Map(), cause: Option[String] = None, timeout: Option[String] = None) =
    createIndex_send(index, settings, mappings, cause, timeout).actionGet

  def createIndex_send(index: String, settings: Map[String, String] = Map(), mappings: Map[String, String] = Map(), cause: Option[String] = None, timeout: Option[String] = None) =
    createIndex_prepare(index, settings, mappings, cause, timeout).execute

  def createIndex_prepare(index: String, settings: Map[String, String] = Map(), mappings: Map[String, String] = Map(), cause: Option[String] = None, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareCreate(index)
    if (!settings.isEmpty) request.setSettings(settingsBuilder.put(settings).build())
    mappings foreach { case (kind, mapping) => request.addMapping(kind, mapping) }
    cause foreach { request.cause(_) }
    timeout foreach { request.setTimeout(_) }
    request
  }
}

trait IndexDelete {
  self: Indexer =>
  def deleteIndex(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndex_send(indices, timeout).actionGet
  def deleteIndex_send(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndex_prepare(indices, timeout).execute
  def deleteIndex_prepare(indices: Iterable[String] = Nil, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareDelete(indices.toArray: _*)
    timeout foreach { request.setTimeout(_) }
    request
  }

  def deleteIndexIfExists(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndexIfExists_send(indices, timeout).actionGet
  def deleteIndexIfExists_send(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndexIfExists_prepare(indices, timeout).execute
  def deleteIndexIfExists_prepare(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndex_prepare(indices filter (exists(_)), timeout)
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

trait Alias {
  self: Indexer =>
  def alias(indices: Iterable[String], alias: String, filter: Map[String, Object] = Map(), timeout: Option[String] = None) = alias_send(indices, alias, filter, timeout).actionGet
  def alias_send(indices: Iterable[String], alias: String, filter: Map[String, Object] = Map(), timeout: Option[String] = None) = alias_prepare(indices, alias, filter, timeout).execute
  def alias_prepare(indices: Iterable[String], alias: String, filter: Map[String, Object] = Map(), timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareAliases
    indices foreach { request.addAlias(_, alias, filter) }
    timeout foreach { each => request.setTimeout(parseTimeValue(each, null)) }
    request
  }
}

trait Unalias {
  self: Indexer =>
  def unalias(indices: Iterable[String], alias: String, timeout: Option[String] = None) = unalias_send(indices, alias, timeout).actionGet
  def unalias_send(indices: Iterable[String], alias: String, timeout: Option[String] = None) = unalias_prepare(indices, alias, timeout).execute
  def unalias_prepare(indices: Iterable[String], alias: String, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareAliases
    indices foreach { request.removeAlias(_, alias) }
    timeout foreach { each => request.setTimeout(parseTimeValue(each, null)) }
    request
  }
}

trait Optimize {
  self: Indexer =>
  def optimize(indices: String*) = optimize_send(indices.toArray: _*).actionGet
  def optimize_send(indices: String*) = optimize_prepare(indices.toArray: _*).execute
  def optimize_prepare(indices: String*) = client.admin.indices.prepareOptimize(indices.toArray: _*)
  //setFlush(boolean)
  //setListenerThreaded(boolean)
  //setMaxNumSegments(int)
  //setOnlyExpungeDeletes(boolean)
  //setOperationThreading(BroadcastOperationThreading)
  //setRefresh(boolean)
  //setWaitForMerge(boolean)  
}

trait Flush {
  self: Indexer =>
  def flush(indices: Iterable[String] = Nil, full: Option[Boolean] = None, refresh: Option[Boolean] = None) = flush_send(indices, full, refresh).actionGet
  def flush_send(indices: Iterable[String] = Nil, full: Option[Boolean] = None, refresh: Option[Boolean] = None) = flush_prepare(indices, full, refresh).execute
  def flush_prepare(indices: Iterable[String] = Nil, full: Option[Boolean] = None, refresh: Option[Boolean] = None) = {
    val request = client.admin.indices.prepareFlush(indices.toArray: _*)
    full foreach { request.setFull(_) }
    refresh foreach { request.setRefresh(_) }
    request
  }
}

trait Refresh {
  self: Indexer =>
  import org.elasticsearch.action.support.broadcast._

  def refresh(indices: Iterable[String] = Nil, listenerThreaded: Option[Boolean] = None, operationThreading: Option[BroadcastOperationThreading] = None, waitForOperations: Option[Boolean] = None) = refresh_send(indices, listenerThreaded, operationThreading, waitForOperations).actionGet
  def refresh_send(indices: Iterable[String] = Nil, listenerThreaded: Option[Boolean] = None, operationThreading: Option[BroadcastOperationThreading] = None, waitForOperations: Option[Boolean] = None) = refresh_prepare(indices, listenerThreaded, operationThreading, waitForOperations).execute
  def refresh_prepare(indices: Iterable[String] = Nil, listenerThreaded: Option[Boolean] = None, operationThreading: Option[BroadcastOperationThreading] = None, waitForOperations: Option[Boolean] = None) = {
    val request = client.admin.indices.prepareRefresh(indices.toArray: _*)
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    waitForOperations foreach { request.setWaitForOperations(_) }
    request
  }
}

trait Status {
  self: Indexer =>
  def status(indices: String*) = status_send(indices.toArray: _*).actionGet
  def status_send(indices: String*) = status_prepare(indices.toArray: _*).execute
  def status_prepare(indices: String*) = client.admin.indices.prepareStatus(indices.toArray: _*)
  //setRecovery(boolean)
  //setSnapshot(boolean)
}

trait Stats {
  self: Indexer =>
  def stats(indices: String*) = stats_send(indices.toArray: _*).actionGet
  def stats_send(indices: String*) = stats_prepare(indices.toArray: _*).execute
  def stats_prepare(indices: String*) = client.admin.indices.prepareStats(indices.toArray: _*)
  //setDocs(boolean)
  //setFlush(boolean)
  //setGet(boolean)
  //setGroups(String...)
  //setIndexing(boolean)
  //setMerge(boolean)
  //setRefresh(boolean)
  //setSearch(boolean)
  //setStore(boolean)
  //setTypes(String...)
}

trait PutMapping {
  self: Indexer =>
  def putMapping(index: String, `type`: String, json: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = putMapping_send(index, `type`, json, ignoreConflicts, timeout).actionGet
  def putMapping_send(index: String, `type`: String, json: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = putMapping_prepare(Seq(index), `type`, json, ignoreConflicts, timeout).execute
  def putMapping_prepare(indices: Iterable[String], `type`: String, json: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = {
    val request = client.admin.indices.preparePutMapping(indices.toArray: _*)
    request.setType(`type`)
    request.setSource(json)
    ignoreConflicts foreach { request.setIgnoreConflicts(_) }
    timeout foreach { request.setTimeout(_) }
    request
  }
}

trait DeleteMapping {
  self: Indexer =>
  def deleteMapping(indices: Iterable[String], `type`: Option[String] = None, timeout: Option[String] = None) = deleteMapping_send(indices, `type`, timeout).actionGet
  def deleteMapping_send(indices: Iterable[String], `type`: Option[String] = None, timeout: Option[String] = None) = deleteMapping_prepare(indices, `type`, timeout).execute
  def deleteMapping_prepare(indices: Iterable[String], `type`: Option[String] = None, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareDeleteMapping(indices.toArray: _*)
    `type` foreach { request.setType(_) }
    timeout foreach { each => request.setMasterNodeTimeout(parseTimeValue(each, null)) }
    request
  }
}