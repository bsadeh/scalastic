package com.traackr.scalastic.elasticsearch

import org.elasticsearch.common._
import org.elasticsearch.index.query._, QueryBuilders._
import org.elasticsearch.action._, index._, bulk._
import org.elasticsearch.action.support.replication._
import scala.collection._, JavaConversions._
import scalaz._, Scalaz._

trait Indexing
    extends Index
    with IndexInBulk
    with Searching
    with Count
    with Get
    with Multiget
    with Delete
    with DeleteByQuery {
  self: Indexer =>
}

trait Index {
  self: Indexer =>

  def index(index: String, `type`: String, id: String, source: String, parent: String = null, ttl: Long = 0, routing: String = "") =
    index_send(index, `type`, id, source, parent, ttl, routing).actionGet

  def index_send(index: String, `type`: String, id: String, source: String, parent: String = null, ttl: Long = 0, routing: String = "") =
    index_prepare(index, `type`, id, source, parent, ttl, routing).execute

  def index_prepare(index: String, `type`: String, id: String, source: String, parent: String = null, ttl: Long = 0, routing: String = "") = {
    val request = client.prepareIndex(index, `type`, id)
    request.setSource(source)
    request.setParent(parent)
    if (ttl > 0) request.setTTL(ttl)
    if (!routing.isEmpty) request.setRouting(routing)
    request
  }
}

trait IndexInBulk {
  self: Indexer =>
  def bulk(requests: Iterable[IndexRequest]) = bulk_send(requests).actionGet
  def bulk_send(requests: Iterable[IndexRequest]) = bulk_prepare(requests).execute
  def bulk_prepare(requests: Iterable[IndexRequest]) = requests.foldLeft(client.prepareBulk)(_.add(_))
}

trait Count {
  self: Indexer =>

  def count(indices: Iterable[String] = Nil, types: Iterable[String] = Nil, query: QueryBuilder = matchAllQuery) =
    count_send(indices, types, query).actionGet.count

  def count_send(indices: Iterable[String] = Nil, types: Iterable[String] = Nil, query: QueryBuilder = matchAllQuery) =
    count_prepare(indices, types, query).execute

  def count_prepare(indices: Iterable[String] = Nil, types: Iterable[String] = Nil, query: QueryBuilder = matchAllQuery) =
    client.prepareCount(indices.toArray: _*)
      .setTypes(types.toArray: _*)
      .setQuery(query)
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
    routing: Option[String] = None) = 
      get_send(index, `type`, id, fields, listenerThreaded, operationThreaded, preference, realtime, refresh, routing).actionGet
      
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
    routing: Option[String] = None) = 
      get_prepare(index, `type`, id, fields, listenerThreaded, operationThreaded, preference, realtime, refresh, routing).execute
      
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
    listenerThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None) = 
    multiget_send(index, `type`, ids, listenerThreaded, preference, realtime, refresh).actionGet
    
  def multiget_send(
    index: String,
    @Nullable `type`: String,
    ids: Iterable[String],
    listenerThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None) = 
    multiget_prepare(index, `type`, ids, listenerThreaded, preference, realtime, refresh).execute
    
  def multiget_prepare(
    index: String,
    @Nullable `type`: String,
    ids: Iterable[String],
    listenerThreaded: Option[Boolean] = None,
    preference: Option[String] = None,
    realtime: Option[Boolean] = None,
    refresh: Option[Boolean] = None) = {
		  /* method body */
    val request = client.prepareMultiGet
    for (each <- ids) request.add(index, `type`, each)
    listenerThreaded foreach { request.setListenerThreaded(_) }
    preference foreach { request.setPreference(_) }
    realtime foreach { request.setRealtime(_) }
    refresh foreach { request.setRefresh(_) }
    request
  }
}

//todo: add parameters with defaults
/*
trait MoreLikeThis {
  self: Indexer =>
  def moreLikeThis(index: String, `type`: String, id: String) = moreLikeThis_send(index, `type`, id).actionGet
  def moreLikeThis_send(index: String, `type`: String, id: String) = moreLikeThis_prepare(index, `type`, id).execute
  def moreLikeThis_prepare(index: String, `type`: String, id: String) = {
    val request = client.prepareMoreLikeThis(index, `type`, id)
    //    request.set
    request
  }
}
*/

trait Update {
  self: Indexer =>

  def update(
    index: String, `type`: String, id: String, parent: Option[String] = None,
    script: Option[String] = None, scriptLanguage: Option[String] = None, scriptParams: Map[String, Object] = Map(),
    percolate: Option[String] = None, replicationType: Option[ReplicationType] = None, consistencyLevel: Option[WriteConsistencyLevel] = None) =
    update_send(index, `type`, id, parent, script, scriptLanguage, scriptParams, percolate, replicationType, consistencyLevel).actionGet

  def update_send(
    index: String, `type`: String, id: String, parent: Option[String] = None,
    script: Option[String] = None, scriptLanguage: Option[String] = None, scriptParams: Map[String, Object] = Map(),
    percolate: Option[String] = None, replicationType: Option[ReplicationType] = None, consistencyLevel: Option[WriteConsistencyLevel] = None) =
    update_prepare(index, `type`, id, parent, script, scriptLanguage, scriptParams, percolate, replicationType, consistencyLevel).execute

  def update_prepare(
    index: String, `type`: String, id: String, parent: Option[String] = None,
    script: Option[String] = None, scriptLanguage: Option[String] = None, scriptParams: Map[String, Object] = Map(),
    percolate: Option[String] = None, replicationType: Option[ReplicationType] = None, consistencyLevel: Option[WriteConsistencyLevel] = None) = {
    val request = client.prepareUpdate(index, `type`, id)
    parent foreach { request.setParent(_) }
    script foreach { that =>
      request.setScript(that)
      request.setScriptParams(scriptParams)
      scriptLanguage foreach { request.setScriptLang(_) }
    }
    percolate foreach { request.setPercolate(_) }
    replicationType foreach { request.setReplicationType(_) }
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    // revisit: replicationType & consistencyLevel
    // should we do this:
    //    request.setReplicationType(replicationType some { that => that } none { ReplicationType.DEFAULT })
    //    request.setConsistencyLevel(consistencyLevel some { that => that } none { WriteConsistencyLevel.DEFAULT })
    request
  }
}

trait Delete {
  self: Indexer =>
  def delete(index: String, `type`: String, id: String) = delete_send(index, `type`, id).actionGet
  def delete_send(index: String, `type`: String, id: String) = delete_prepare(index, `type`, id).execute
  def delete_prepare(index: String, `type`: String, id: String) = client.prepareDelete(index, `type`, id)
}

trait DeleteByQuery {
  self: Indexer =>

  def deleteByQuery(indices: Iterable[String] = Nil, types: Iterable[String] = Nil, query: QueryBuilder = matchAllQuery,
    replicationType: Option[ReplicationType] = None, consistencyLevel: Option[WriteConsistencyLevel] = None,
    routing: String = "", timeout: String = "") =
    deleteByQuery_send(indices, types, query, replicationType, consistencyLevel, routing, timeout).actionGet

  def deleteByQuery_send(indices: Iterable[String] = Nil, types: Iterable[String] = Nil, query: QueryBuilder = matchAllQuery,
    replicationType: Option[ReplicationType] = None, consistencyLevel: Option[WriteConsistencyLevel] = None,
    routing: String = "", timeout: String = "") =
    deleteByQuery_prepare(indices, types, query, replicationType, consistencyLevel, routing, timeout).execute

  def deleteByQuery_prepare(indices: Iterable[String] = Nil, types: Iterable[String] = Nil, query: QueryBuilder = matchAllQuery,
    replicationType: Option[ReplicationType] = None, consistencyLevel: Option[WriteConsistencyLevel] = None,
    routing: String = "", timeout: String = "") = {
    val request = client.prepareDeleteByQuery(indices.toArray: _*)
    request.setTypes(types.toArray: _*)
    request.setQuery(query)
    replicationType foreach { request.setReplicationType(_) }
    consistencyLevel foreach { request.setConsistencyLevel(_) }
    // revisit: replicationType & consistencyLevel
    // should we do this:
    //    request.setReplicationType(replicationType some { that => that } none { ReplicationType.DEFAULT })
    //    request.setConsistencyLevel(consistencyLevel some { that => that } none { WriteConsistencyLevel.DEFAULT })
    if (!routing.isEmpty) request.setRouting(routing)
    if (!timeout.isEmpty) request.setTimeout(timeout)
    request
  }
}
