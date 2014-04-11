package scalastic.elasticsearch

import org.elasticsearch.action.support.broadcast._
import org.elasticsearch.common.settings.ImmutableSettings._
import org.elasticsearch.cluster.metadata._
import org.elasticsearch.index.query._
import org.elasticsearch.common.unit._, TimeValue._
import scala.collection._, JavaConversions._
import org.elasticsearch.Conversions._
import org.elasticsearch.action.admin.indices.cache.clear._

trait IndexCrud
    extends Exists
    with CreateIndex with DeleteIndex
    with Open with Close
    with Alias with Unalias
    with PutMapping with DeleteMapping
    with GetFieldMappings
    with PutTemplate with DeleteTemplate
    with GetTemplates
    with ClearCache
    with Flush
    with Refresh
    with Optimize
    with GatewaySnapshot
    with Segments
    with Status
    with Stats
    with UpdateSettings {
  self: Indexer =>
}

trait CreateIndex {
  self: Indexer =>

  def createIndex(
    index: String,
//fixme: change settings to  consistently be a map, or a json string
    settings: Map[String, String] = Map(),
    mappings: Map[String, String] = Map(),
    cause: Option[String] = None,
    timeout: Option[String] = None) = createIndex_send(index, settings, mappings, cause, timeout).actionGet

  def createIndex_send(
    index: String,
    settings: Map[String, String] = Map(),
    mappings: Map[String, String] = Map(),
    cause: Option[String] = None,
    timeout: Option[String] = None) = createIndex_prepare(index, settings, mappings, cause, timeout).execute

  def createIndex_prepare(
    index: String,
    settings: Map[String, String] = Map(),
    mappings: Map[String, String] = Map(),
    cause: Option[String] = None,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.admin.indices.prepareCreate(index)
    if (!settings.isEmpty) request.setSettings(settingsBuilder.put(settings.toSettings).build())
    mappings foreach { case (kind, mapping) => request.addMapping(kind, mapping) }
    cause foreach { request.setCause(_) }
    timeout foreach { request.setTimeout(_) }
    request
  }
}

trait DeleteIndex {
  self: Indexer =>
  def deleteIndex(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndex_send(indices, timeout).actionGet
  def deleteIndex_send(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndex_prepare(indices, timeout).execute
  def deleteIndex_prepare(indices: Iterable[String] = Nil, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareDelete(indices.toArray: _*)
    timeout foreach { request.setTimeout(_) }
    request
  }

  /** caveat: if none of the indices exists, we will encounter a validation exception */
  def deleteIndexIfExists(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndexIfExists_send(indices, timeout).actionGet
  def deleteIndexIfExists_send(indices: Iterable[String] = Nil, timeout: Option[String] = None) = deleteIndexIfExists_prepare(indices, timeout).execute
  def deleteIndexIfExists_prepare(indices: Iterable[String] = Nil, timeout: Option[String] = None) = {
    val existing = indices filter (exists(_).isExists)
    val toDelete: Iterable[String] = if (existing.isEmpty) null else existing
    deleteIndex_prepare(toDelete, timeout)
  }
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
  def exists(indices: String*) = exists_send(indices.toArray: _*).actionGet
  def exists_send(indices: String*) = exists_prepare(indices.toArray: _*).execute
  def exists_prepare(indices: String*) = client.admin.indices.prepareExists(indices.toArray: _*)
}

trait Alias {
  self: Indexer =>

  def alias(
    indices: Iterable[String],
    alias: String,
    actions: Iterable[AliasAction] = Nil,
    filter: Option[FilterBuilder] = None,
    timeout: Option[String] = None) = alias_send(indices, alias, actions, filter, timeout).actionGet

  def alias_send(
    indices: Iterable[String],
    alias: String,
    actions: Iterable[AliasAction] = Nil,
    filter: Option[FilterBuilder] = None,
    timeout: Option[String] = None) = alias_prepare(indices, alias, actions, filter, timeout).execute

  def alias_prepare(
    indices: Iterable[String],
    alias: String,
    actions: Iterable[AliasAction] = Nil,
    filter: Option[FilterBuilder] = None,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.admin.indices.prepareAliases
    actions foreach { request.addAliasAction(_) }
    indices foreach {
      filter match {
        case Some(builder) => request.addAlias(_, alias, builder)
        case None => request.addAlias(_, alias)
      }
    }
    timeout foreach { each => request.setTimeout(parseTimeValue(each, null)) }
    request
  }
}

trait Unalias {
  self: Indexer =>

  def unalias(
    indices: Iterable[String],
    alias: String,
    timeout: Option[String] = None) = unalias_send(indices, alias, timeout).actionGet

  def unalias_send(
    indices: Iterable[String],
    alias: String,
    timeout: Option[String] = None) = unalias_prepare(indices, alias, timeout).execute

  def unalias_prepare(
    indices: Iterable[String],
    alias: String,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.admin.indices.prepareAliases
    indices foreach { request.removeAlias(_, alias) }
    timeout foreach { each => request.setTimeout(parseTimeValue(each, null)) }
    request
  }
}

trait Optimize {
  self: Indexer =>

  def optimize(
    indices: Iterable[String] = Nil,
    flush: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    maxNumSegments: Option[Int] = None,
    onlyExpungeDeletes: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    waitForMerge: Option[Boolean] = None) =
    optimize_send(indices, flush, listenerThreaded, maxNumSegments, onlyExpungeDeletes, operationThreading, waitForMerge).actionGet

  def optimize_send(
    indices: Iterable[String] = Nil,
    flush: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    maxNumSegments: Option[Int] = None,
    onlyExpungeDeletes: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    waitForMerge: Option[Boolean] = None) =
    optimize_prepare(indices, flush, listenerThreaded, maxNumSegments, onlyExpungeDeletes, operationThreading, waitForMerge).execute

  def optimize_prepare(
    indices: Iterable[String] = Nil,
    flush: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    maxNumSegments: Option[Int] = None,
    onlyExpungeDeletes: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    waitForMerge: Option[Boolean] = None) = {
      /* method body */
    val request = client.admin.indices.prepareOptimize(indices.toArray: _*)
    flush foreach { request.setFlush(_) }
    listenerThreaded foreach { request.setListenerThreaded(_) }
    maxNumSegments foreach { request.setMaxNumSegments(_) }
    onlyExpungeDeletes foreach { request.setOnlyExpungeDeletes(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    waitForMerge foreach { request.setWaitForMerge(_) }
    request
  }
}

trait Flush {
  self: Indexer =>

  def flush(
    indices: Iterable[String] = Nil,
    full: Option[Boolean] = None) = flush_send(indices, full).actionGet

  def flush_send(
    indices: Iterable[String] = Nil,
    full: Option[Boolean] = None) = flush_prepare(indices, full).execute

  def flush_prepare(
    indices: Iterable[String] = Nil,
    full: Option[Boolean] = None) = {
      /* method body */
    val request = client.admin.indices.prepareFlush(indices.toArray: _*)
    full foreach { request.setFull(_) }
    request
  }
}

trait Refresh {
  self: Indexer =>

  def refresh(
    indices: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    force: Option[Boolean] = None) = refresh_send(indices, listenerThreaded, operationThreading, force).actionGet

  def refresh_send(
    indices: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    force: Option[Boolean] = None) = refresh_prepare(indices, listenerThreaded, operationThreading, force).execute

  def refresh_prepare(
    indices: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    force: Option[Boolean] = None) = {
      /* method body */
    val request = client.admin.indices.prepareRefresh(indices.toArray: _*)
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    force foreach { request.setForce(_) }
    request
  }
}

trait Status {
  self: Indexer =>
  def status(indices: Iterable[String] = Nil, recovery: Option[Boolean] = None, snapshot: Option[Boolean] = None) = status_send(indices, recovery, snapshot).actionGet
  def status_send(indices: Iterable[String] = Nil, recovery: Option[Boolean] = None, snapshot: Option[Boolean] = None) = status_prepare(indices, recovery, snapshot).execute
  def status_prepare(indices: Iterable[String] = Nil, recovery: Option[Boolean] = None, snapshot: Option[Boolean] = None) = {
      /* method body */
    val request = client.admin.indices.prepareStatus(indices.toArray: _*)
    recovery foreach { request.setRecovery(_) }
    snapshot foreach { request.setSnapshot(_) }
    request
  }
}

trait Stats {
  self: Indexer =>

  def stats(
    indices: Iterable[String] = Nil,
    docs: Option[Boolean] = None,
    flush: Option[Boolean] = None,
    get: Option[Boolean] = None,
    groups: Iterable[String] = Nil,
    indexing: Option[Boolean] = None,
    merge: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    search: Option[Boolean] = None,
    store: Option[Boolean] = None,
    types: Iterable[String] = Nil) = stats_send(indices, docs, flush, get, groups, indexing, merge, refresh, search, store, types).actionGet

  def stats_send(
    indices: Iterable[String] = Nil,
    docs: Option[Boolean] = None,
    flush: Option[Boolean] = None,
    get: Option[Boolean] = None,
    groups: Iterable[String] = Nil,
    indexing: Option[Boolean] = None,
    merge: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    search: Option[Boolean] = None,
    store: Option[Boolean] = None,
    types: Iterable[String] = Nil) = stats_prepare(indices, docs, flush, get, groups, indexing, merge, refresh, search, store, types).execute

  def stats_prepare(
    indices: Iterable[String] = Nil,
    docs: Option[Boolean] = None,
    flush: Option[Boolean] = None,
    get: Option[Boolean] = None,
    groups: Iterable[String] = Nil,
    indexing: Option[Boolean] = None,
    merge: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    search: Option[Boolean] = None,
    store: Option[Boolean] = None,
    types: Iterable[String] = Nil,
    completion: Option[Boolean] = None,
    completionFields: Iterable[String] = Nil,
    segments: Option[Boolean] = None) = {
      /* method body */
    val request = client.admin.indices.prepareStats(indices.toArray: _*)
    docs foreach { request.setDocs(_) }
    flush foreach { request.setFlush(_) }
    get foreach { request.setGet(_) }
    request.setGroups(groups.toArray: _*)
    indexing foreach { request.setIndexing(_) }
    merge foreach { request.setMerge(_) }
    refresh foreach { request.setRefresh(_) }
    search foreach { request.setSearch(_) }
    store foreach { request.setStore(_) }
    request.setTypes(types.toArray: _*)
    completion foreach { request.setCompletion(_) }
    request.setCompletionFields(completionFields.toArray: _*)
    segments foreach { request.setSegments(_) }
    request
  }
}

trait PutMapping {
  self: Indexer =>

  def putMapping(index: String, `type`: String, source: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = 
    putMappingForAll(Seq(index), `type`, source, ignoreConflicts, timeout)
  def putMapping_send(index: String, `type`: String, source: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = 
    putMappingForAll_send(Seq(index), `type`, source, ignoreConflicts, timeout)
  def putMapping_prepare(index: String, `type`: String, source: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = 
    putMappingForAll_prepare(Seq(index), `type`, source, ignoreConflicts, timeout)

  def putMappingForAll(indices: Iterable[String], `type`: String, source: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = 
    putMappingForAll_send(indices, `type`, source, ignoreConflicts, timeout).actionGet
  def putMappingForAll_send(indices: Iterable[String], `type`: String, source: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = 
    putMappingForAll_prepare(indices, `type`, source, ignoreConflicts, timeout).execute
  def putMappingForAll_prepare(indices: Iterable[String], `type`: String, source: String, ignoreConflicts: Option[Boolean] = None, timeout: Option[String] = None) = {
    val request = client.admin.indices.preparePutMapping(indices.toArray: _*)
    request.setType(`type`)
    request.setSource(source)
    ignoreConflicts foreach { request.setIgnoreConflicts(_) }
    timeout foreach { request.setTimeout(_) }
    request
  }
}

trait GetFieldMappings {
  self: Indexer =>

  def getFieldMappings(
    indices: Iterable[String] = Nil,
    fields: Iterable[String] = Nil,
    includeDefaults: Option[Boolean] = None) =  getFieldMappings_send(indices, fields, includeDefaults).actionGet

  def getFieldMappings_send(
    indices: Iterable[String] = Nil,
    fields: Iterable[String] = Nil,
    includeDefaults: Option[Boolean] = None) = getFieldMappings_prepare(indices, fields, includeDefaults).execute

  def getFieldMappings_prepare(
    indices: Iterable[String] = Nil,
    fields: Iterable[String] = Nil,
    includeDefaults: Option[Boolean] = None) = {
      /* method body */
    val request = client.admin.indices.prepareGetFieldMappings(indices.toArray: _*)
    request.setFields(fields.toArray: _*)
    includeDefaults foreach { request.includeDefaults(_) }
    request
  }
}

trait DeleteMapping {
  self: Indexer =>

  def deleteMapping(
    indices: Iterable[String] = Nil,
    `type`: Option[String] = None,
    timeout: Option[String] = None) =  deleteMapping_send(indices, `type`, timeout).actionGet

  def deleteMapping_send(
    indices: Iterable[String] = Nil,
    `type`: Option[String] = None,
    timeout: Option[String] = None) = deleteMapping_prepare(indices, `type`, timeout).execute

  def deleteMapping_prepare(
    indices: Iterable[String] = Nil,
    `type`: Option[String] = None,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.admin.indices.prepareDeleteMapping(indices.toArray: _*)
    `type` foreach { request.setType(_) }
    timeout foreach { each => request.setMasterNodeTimeout(parseTimeValue(each, null)) }
    request
  }
}

trait PutTemplate {
  self: Indexer =>

  def putTemplate(
    name: String,
    settings: Map[String, String] = Map(),
    mappings: Map[String, String] = Map(),
    cause: Option[String] = None,
    create: Option[Boolean] = None,
    order: Option[Int] = None,
    source: Option[String] = None,
    template: Option[String] = None,
    timeout: Option[String] = None) =
    putTemplate_send(name, settings, mappings, cause, create, order, source, template, timeout).actionGet

  def putTemplate_send(
    name: String,
    settings: Map[String, String] = Map(),
    mappings: Map[String, String] = Map(),
    cause: Option[String] = None,
    create: Option[Boolean] = None,
    order: Option[Int] = None,
    source: Option[String] = None,
    template: Option[String] = None,
    timeout: Option[String] = None) =
    putTemplate_prepare(name, settings, mappings, cause, create, order, source, template, timeout).execute

  def putTemplate_prepare(
    name: String,
    settings: Map[String, String] = Map(),
    mappings: Map[String, String] = Map(),
    cause: Option[String] = None,
    create: Option[Boolean] = None,
    order: Option[Int] = None,
    source: Option[String] = None,
    template: Option[String] = None,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.admin.indices.preparePutTemplate(name)
    if (!settings.isEmpty) request.setSettings(settingsBuilder.put(settings.toSettings).build())
    mappings foreach { case (kind, mapping) => request.addMapping(kind, mapping) }
    cause foreach { request.cause(_) }
    create foreach { request.setCreate(_) }
    order foreach { request.setOrder(_) }
    source foreach { request.setSource(_) }
    template foreach { request.setTemplate(_) }
    timeout foreach { request.setMasterNodeTimeout(_) }
    request
  }
}

trait DeleteTemplate {
  self: Indexer =>
  def deleteTemplate(name: String, timeout: Option[String] = None) = deleteTemplate_send(name, timeout).actionGet
  def deleteTemplate_send(name: String, timeout: Option[String] = None) = deleteTemplate_prepare(name, timeout).execute
  def deleteTemplate_prepare(name: String, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareDeleteTemplate(name)
    timeout foreach { request.setMasterNodeTimeout(_) }
    request
  }
}

trait GetTemplates {
  self: Indexer =>
  def getTemplates(names: Iterable[String]) = getTemplates_send(names).actionGet
  def getTemplates_send(names: Iterable[String]) = getTemplates_prepare(names).execute
  def getTemplates_prepare(names: Iterable[String]) = {
    val request = client.admin.indices.prepareGetTemplates(names.toArray: _*)
    request
  }
}

trait Open {
  self: Indexer =>
  def openIndex(index: String, timeout: Option[String] = None) = openIndex_send(index, timeout).actionGet
  def openIndex_send(index: String, timeout: Option[String] = None) = openIndex_prepare(index, timeout).execute
  def openIndex_prepare(index: String, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareOpen(index)
    timeout foreach { request.setTimeout(_) }
    request
  }
}

trait Close {
  self: Indexer =>
  def closeIndex(index: String, timeout: Option[String] = None) = closeIndex_send(index, timeout).actionGet
  def closeIndex_send(index: String, timeout: Option[String] = None) = closeIndex_prepare(index, timeout).execute
  def closeIndex_prepare(index: String, timeout: Option[String] = None) = {
    val request = client.admin.indices.prepareClose(index)
    timeout foreach { request.setTimeout(_) }
    request
  }
}

@deprecated(message="use the new snapshot/restore API instead", since="1.1.0")
trait GatewaySnapshot {
  self: Indexer =>
  def gatewaySnapshot(indices: String*) = gatewaySnapshot_send(indices.toArray: _*).actionGet
  def gatewaySnapshot_send(indices: String*) = gatewaySnapshot_prepare(indices.toArray: _*).execute
  def gatewaySnapshot_prepare(indices: String*) = client.admin.indices.prepareGatewaySnapshot(indices.toArray: _*)
}

trait Segments {
  self: Indexer =>
  def segments(indices: String*) = segments_send(indices.toArray: _*).actionGet
  def segments_send(indices: String*) = segments_prepare(indices.toArray: _*).execute
  def segments_prepare(indices: String*) = client.admin.indices.prepareSegments(indices.toArray: _*)
}

trait ClearCache {
  self: Indexer =>

  def clearCache(
    indices: Iterable[String] = Nil,
    fields: Iterable[String] = Nil,
    fieldDataCache: Option[Boolean] = None,
    filterCache: Option[Boolean] = None,
    idCache: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None) = clearCache_send(indices, fields, fieldDataCache, filterCache, idCache, listenerThreaded, operationThreading).actionGet

  def clearCache_send(
    indices: Iterable[String] = Nil,
    fields: Iterable[String] = Nil,
    fieldDataCache: Option[Boolean] = None,
    filterCache: Option[Boolean] = None,
    idCache: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None) = clearCache_prepare(indices, fields, fieldDataCache, filterCache, idCache, listenerThreaded, operationThreading).execute

  def clearCache_prepare(
    indices: Iterable[String] = Nil,
    fields: Iterable[String] = Nil,
    fieldDataCache: Option[Boolean] = None,
    filterCache: Option[Boolean] = None,
    idCache: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None): ClearIndicesCacheRequestBuilder = {
      /* method body */
    val request = client.admin.indices.prepareClearCache(indices.toArray: _*)
    request.setFields(fields.toArray: _*)
    fieldDataCache foreach { request.setFieldDataCache(_) }
    filterCache foreach { request.setFilterCache(_) }
    idCache foreach { request.setIdCache(_) }
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    request
  }
}
