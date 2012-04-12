package com.traackr.elasticsearch

import org.elasticsearch.common._
import org.elasticsearch.index.query._, QueryBuilders._
import org.elasticsearch.action._, index._, bulk._
import org.elasticsearch.action.support.replication._
import net.liftweb.json._

trait Indexing extends Index with IndexInBulk with Searching with Count with Get with Multiget with Delete {
  self: Indexer =>
}

trait Index {
  self: Indexer =>

  def index(index: String, `type`: String, id: String, json: String, parent: String = null, routing: String = "") = {
    index_send(index, `type`, id, json, parent, routing).actionGet
  }

  def index_send(index: String, `type`: String, id: String, json: String, parent: String = null, routing: String = "") = {
    index_prepare(index, `type`, id, json, parent, routing).execute
  }

  def index_prepare(index: String, `type`: String, id: String, json: String, parent: String = null, routing: String = "") = {
    val request = client.prepareIndex(index, `type`, id)
    request.setSource(pretty(render(parse(json))))
    request.setParent(parent)
    if (!routing.isEmpty) request.setRouting(routing)
    request
  }

  def index(builder: IndexRequestBuilder) = index_send(builder).actionGet
  def index_send(builder: IndexRequestBuilder) = builder.execute
}

trait IndexInBulk {
  self: Indexer =>
  def bulk(requests: Seq[IndexRequest]) = bulk_send(requests).actionGet
  def bulk_send(requests: Seq[IndexRequest]) = bulk_prepare(requests).execute
  def bulk_prepare(requests: Seq[IndexRequest]) = {
    val request = client.prepareBulk
    requests foreach { request.add(_) }
    request
  }
}

trait Count {
  self: Indexer =>

  def count(indices: Seq[String] = Seq(), types: Seq[String] = Seq(), query: QueryBuilder = matchAllQuery) =
    count_send(indices, types, query).actionGet.count

  def count_send(indices: Seq[String] = Seq(), types: Seq[String] = Seq(), query: QueryBuilder = matchAllQuery) =
    count_prepare(indices, types, query).execute

  def count_prepare(indices: Seq[String] = Seq(), types: Seq[String] = Seq(), query: QueryBuilder = matchAllQuery) = {
    client.prepareCount(indices.toArray: _*)
      .setTypes(types.toArray: _*)
      .setQuery(query)
  }
}

trait Get {
  self: Indexer =>
  def get(index: String, @Nullable `type`: String, id: String) = get_send(index, `type`, id).actionGet
  def get_send(index: String, @Nullable `type`: String, id: String) = get_prepare(index, `type`, id).execute
  def get_prepare(index: String, @Nullable `type`: String, id: String) = client.prepareGet(index, `type`, id)
}

trait Multiget {
  self: Indexer =>
  def get(index: String, @Nullable `type`: String, ids: Seq[String]) = get_send(index, `type`, ids).actionGet
  def get_send(index: String, @Nullable `type`: String, ids: Seq[String]) = get_prepare(index, `type`, ids).execute
  def get_prepare(index: String, @Nullable `type`: String, ids: Seq[String]) = {
    val request = client.prepareMultiGet
    for (each <- ids) request.add(index, `type`, each)
    request
  }
}

//todo: add parameters with defaults
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

//todo: add script+lang+parameters
trait Update {
  self: Indexer =>
  def update(index: String, `type`: String, id: String, parent: String = null, percolate: String = "*", replicationType: ReplicationType = ReplicationType.DEFAULT, consistencyLevel: WriteConsistencyLevel = WriteConsistencyLevel.DEFAULT) =
    update_send(index, `type`, id, parent, percolate, replicationType, consistencyLevel).actionGet
  def update_send(index: String, `type`: String, id: String, parent: String = null, percolate: String = "*", replicationType: ReplicationType = ReplicationType.DEFAULT, consistencyLevel: WriteConsistencyLevel = WriteConsistencyLevel.DEFAULT) =
    update_prepare(index, `type`, id, parent, percolate, replicationType, consistencyLevel).execute
  def update_prepare(index: String, `type`: String, id: String, parent: String = null, percolate: String = "*", replicationType: ReplicationType = ReplicationType.DEFAULT, consistencyLevel: WriteConsistencyLevel = WriteConsistencyLevel.DEFAULT) = {
    val request = client.prepareUpdate(index, `type`, id)
    request.setParent(parent)
    request.setPercolate(percolate)
    request.setReplicationType(replicationType)
    request.setConsistencyLevel(consistencyLevel)
    request
  }
}

trait Delete {
  self: Indexer =>
  def delete(index: String, `type`: String, id: String) = delete_send(index, `type`, id).actionGet
  def delete_send(index: String, `type`: String, id: String) = delete_prepare(index, `type`, id).execute
  def delete_prepare(index: String, `type`: String, id: String) = client.prepareDelete(index, `type`, id)
}
