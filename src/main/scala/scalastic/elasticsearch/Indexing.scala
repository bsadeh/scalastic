package scalastic.elasticsearch

import org.elasticsearch.common._, xcontent._
import org.elasticsearch.index.VersionType
import org.elasticsearch.index.query._, QueryBuilders._
import org.elasticsearch.action._, get._, index._, delete._
import org.elasticsearch.action.support.broadcast._
import org.elasticsearch.action.support.replication._
import scala.collection._, JavaConversions._

trait Indexing
    extends Index
    with Bulk
    with Searching
    with Count
    with Get
    with Multiget
    with Update
    with Delete
    with DeleteByQuery {
  self: Indexer =>
}

trait Index {
  self: Indexer =>

  def index(
    index: String,
    `type`: String,
    @Nullable id: String,
    source: String,
    parent: String = null,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    contentType: Option[XContentType] = None,
    create: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    opType: Option[IndexRequest.OpType] = None,
    percolate: Option[String] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None,
    routing: Option[String] = None,
    timeout: Option[String] = None,
    timestamp: Option[String] = None,
    ttl: Option[Long] = None,
    version: Option[Long] = None,
    versionType: Option[VersionType] = None) = index_send(index, `type`, id, source, parent, consistencyLevel, contentType, create, listenerThreaded, operationThreaded, opType, percolate, refresh, replicationType, routing, timeout, timestamp, ttl, version, versionType).actionGet

  def index_send(
    index: String,
    `type`: String,
    @Nullable id: String,
    source: String,
    parent: String = null,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    contentType: Option[XContentType] = None,
    create: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    opType: Option[IndexRequest.OpType] = None,
    percolate: Option[String] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None,
    routing: Option[String] = None,
    timeout: Option[String] = None,
    timestamp: Option[String] = None,
    ttl: Option[Long] = None,
    version: Option[Long] = None,
    versionType: Option[VersionType] = None) = index_prepare(index, `type`, id, source, parent, consistencyLevel, contentType, create, listenerThreaded, operationThreaded, opType, refresh, replicationType, routing, timeout, timestamp, ttl, version, versionType).execute

  def index_prepare(
    index: String,
    `type`: String,
    @Nullable id: String,
    source: String,
    parent: String = null,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    contentType: Option[XContentType] = None,
    create: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    opType: Option[IndexRequest.OpType] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None,
    routing: Option[String] = None,
    timeout: Option[String] = None,
    timestamp: Option[String] = None,
    ttl: Option[Long] = None,
    version: Option[Long] = None,
    versionType: Option[VersionType] = None) = {
      /* method body */
    val request = client.prepareIndex(index, `type`, id)
    request.setSource(source)
    request.setParent(parent)
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    contentType foreach { request.setContentType(_) }
    create foreach { request.setCreate(_) }
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreaded foreach { request.setOperationThreaded(_) }
    opType foreach { request.setOpType(_) }
    refresh foreach { request.setRefresh(_) }
    replicationType foreach { request.setReplicationType(_) }
    routing foreach { request.setRouting(_) }
    timeout foreach { request.setTimeout(_) }
    timestamp foreach { request.setTimestamp(_) }
    ttl foreach { request.setTTL(_) }
    version foreach { request.setVersion(_) }
    versionType foreach { request.setVersionType(_) }
    request
  }
}

trait Bulk {
  self: Indexer =>

  def bulk[A <: ActionRequest[A]](
    requests: Iterable[A],
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None) = bulk_send(requests, consistencyLevel, refresh, replicationType).actionGet

  def bulk_send[A <: ActionRequest[A]](
    requests: Iterable[A],
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None) = bulk_prepare(requests, consistencyLevel, refresh, replicationType).execute

  def bulk_prepare[A <: ActionRequest[A]](
    requests: Iterable[A],
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None) = {
      /* method body */
    val request = client.prepareBulk
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    refresh foreach { request.setRefresh(_) }
    replicationType foreach { request.setReplicationType(_) }
    requests.foldLeft(request) { (result, each) =>
      each match {
        case indexing: IndexRequest => result.add(indexing)
        case deleting: DeleteRequest => result.add(deleting)
        case other => throw new IllegalArgumentException("%s type can not be bulk-indexed".format(other.getClass))
      }
    }
    request
  }
}

trait Count {
  self: Indexer =>

  def count(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    listenerThreaded: Option[Boolean] = None,
    minScore: Option[Float] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    routing: Option[String] = None) = count_send(indices, types, query, listenerThreaded, minScore, operationThreading, routing).actionGet

  def count_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    listenerThreaded: Option[Boolean] = None,
    minScore: Option[Float] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    routing: Option[String] = None) = count_prepare(indices, types, query, listenerThreaded, minScore, operationThreading, routing).execute

  def count_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    listenerThreaded: Option[Boolean] = None,
    minScore: Option[Float] = None,
    operationThreading: Option[BroadcastOperationThreading] = None,
    routing: Option[String] = None) = {
      /* method body */
    val request = client.prepareCount(indices.toArray: _*)
      .setTypes(types.toArray: _*)
      .setQuery(query)
    listenerThreaded foreach { request.setListenerThreaded(_) }
    minScore foreach { request.setMinScore(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    routing foreach { request.setRouting(_) }
    request
  }
}

trait Get {
  self: Indexer =>
  def get(
    index: String,
    @Nullable `type`: String,
    id: String,
    fields: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    routing: Option[String] = None) = get_send(index, `type`, id, fields, listenerThreaded, operationThreaded, preference, realtime, refresh, routing).actionGet

    def get_send(
    index: String,
    @Nullable `type`: String,
    id: String,
    fields: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    routing: Option[String] = None) = get_prepare(index, `type`, id, fields, listenerThreaded, operationThreaded, preference, realtime, refresh, routing).execute

    def get_prepare(
    index: String,
    @Nullable `type`: String,
    id: String,
    fields: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    routing: Option[String] = None) = {
      /* method body */
    val request = client.prepareGet(index, `type`, id)
    if (!fields.isEmpty) request.setFields(fields.toArray: _*)
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreaded foreach { request.setOperationThreaded(_) }
    preference foreach { request.setPreference(_) }
    realtime foreach { request.setRealtime(_) }
    refresh foreach { request.setRefresh(_) }
    routing foreach { request.setRouting(_) }
    request
  }
}

trait Multiget {
  self: Indexer =>
  def multiget(
    index: String,
    @Nullable `type`: String,
    ids: Iterable[String],
    fields: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None) = multiget_send(index, `type`, ids, fields, listenerThreaded, preference, realtime, refresh).actionGet

  def multiget_send(
    index: String,
    @Nullable `type`: String,
    ids: Iterable[String],
    fields: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None) = multiget_prepare(index, `type`, ids, fields, listenerThreaded, preference, realtime, refresh).execute

  def multiget_prepare(
    index: String,
    @Nullable `type`: String,
    ids: Iterable[String],
    fields: Iterable[String] = Nil,
    listenerThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None) = {
      /* method body */
    val request = client.prepareMultiGet
    for (each <- ids) {
      val item = new MultiGetRequest.Item(index, `type`, each)
      if (!fields.isEmpty) item.fields(fields.toArray: _*)
      request.add(item)
    }
    listenerThreaded foreach { request.setListenerThreaded(_) }
    preference foreach { request.setPreference(_) }
    realtime foreach { request.setRealtime(_) }
    refresh foreach { request.setRefresh(_) }
    request
  }
}

trait Update {
  self: Indexer =>

  def update(
    index: String, 
    `type`: String, 
    id: String, 
    doc: Option[String] = None,
    parent: Option[String] = None,
    script: Option[String] = None, 
    scriptLanguage: Option[String] = None, 
    scriptParams: Map[String, Object] = Map(),
    percolate: Option[String] = None, 
    replicationType: Option[ReplicationType] = None, 
    consistencyLevel: Option[WriteConsistencyLevel] = None) = update_send(index, `type`, id, doc, parent, script, scriptLanguage, scriptParams, percolate, replicationType, consistencyLevel).actionGet

  def update_send(
    index: String, 
    `type`: String, 
    id: String, 
    doc: Option[String] = None,
    parent: Option[String] = None,
    script: Option[String] = None, 
    scriptLanguage: Option[String] = None, 
    scriptParams: Map[String, Object] = Map(),
    percolate: Option[String] = None, 
    replicationType: Option[ReplicationType] = None, 
    consistencyLevel: Option[WriteConsistencyLevel] = None) = update_prepare(index, `type`, id, doc, parent, script, scriptLanguage, scriptParams, percolate, replicationType, consistencyLevel).execute

  def update_prepare(
    index: String, 
    `type`: String, 
    id: String, 
    doc: Option[String] = None,
    parent: Option[String] = None,
    script: Option[String] = None, 
    scriptLanguage: Option[String] = None, 
    scriptParams: Map[String, Object] = Map(),
    percolate: Option[String] = None, 
    replicationType: Option[ReplicationType] = None, 
    consistencyLevel: Option[WriteConsistencyLevel] = None) = {
          /* method body */
    val request = client.prepareUpdate(index, `type`, id)
    parent foreach { request.setParent(_) }
    doc foreach { request.setDoc(_) }
    script foreach { that =>
      request.setScript(that)
      request.setScriptParams(scriptParams)
      scriptLanguage foreach { request.setScriptLang(_) }
    }
    replicationType foreach { request.setReplicationType(_) }
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    request
  }
}

trait Delete {
  self: Indexer =>

  def delete(
    index: String,
    `type`: String,
    id: String,
    parent: String = null,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None,
    routing: Option[String] = None,
    version: Option[Long] = None,
    versionType: Option[VersionType] = None) = delete_send(index, `type`, id, parent, consistencyLevel, listenerThreaded, operationThreaded, refresh, replicationType, routing, version, versionType).actionGet

  def delete_send(
    index: String,
    `type`: String,
    id: String,
    parent: String = null,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None,
    routing: Option[String] = None,
    version: Option[Long] = None,
    versionType: Option[VersionType] = None) = delete_prepare(index, `type`, id, parent, consistencyLevel, listenerThreaded, operationThreaded, refresh, replicationType, routing, version, versionType).execute

  def delete_prepare(
    index: String,
    `type`: String,
    id: String,
    parent: String = null,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[Boolean] = None,
    refresh: Option[Boolean] = None,
    replicationType: Option[ReplicationType] = None,
    routing: Option[String] = None,
    version: Option[Long] = None,
    versionType: Option[VersionType] = None) = {
      /* method body */
    val request = client.prepareDelete(index, `type`, id)
    request.setParent(parent)
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreaded foreach { request.setOperationThreaded(_) }
    refresh foreach { request.setRefresh(_) }
    replicationType foreach { request.setReplicationType(_) }
    routing foreach { request.setRouting(_) }
    version foreach { request.setVersion(_) }
    versionType foreach { request.setVersionType(_) }
    request
  }
}

trait DeleteByQuery {
  self: Indexer =>

  def deleteByQuery(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    replicationType: Option[ReplicationType] = None,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    routing: Option[String] = None,
    timeout: Option[String] = None) = deleteByQuery_send(indices, types, query, replicationType, consistencyLevel, routing, timeout).actionGet

  def deleteByQuery_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    replicationType: Option[ReplicationType] = None,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    routing: Option[String] = None,
    timeout: Option[String] = None) = deleteByQuery_prepare(indices, types, query, replicationType, consistencyLevel, routing, timeout).execute

  def deleteByQuery_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    replicationType: Option[ReplicationType] = None,
    consistencyLevel: Option[WriteConsistencyLevel] = None,
    routing: Option[String] = None,
    timeout: Option[String] = None) = {
      /* method body */
    val request = client.prepareDeleteByQuery(indices.toArray: _*)
    request.setTypes(types.toArray: _*)
    request.setQuery(query)
    replicationType foreach { request.setReplicationType(_) }
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    routing foreach { request.setRouting(_) }
    timeout foreach { request.setTimeout(_) }
    request
  }
}
