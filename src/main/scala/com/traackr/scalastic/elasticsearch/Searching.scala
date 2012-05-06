package com.traackr.scalastic.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.index.query._, QueryBuilders._
import org.elasticsearch.common.xcontent._
import org.elasticsearch.search._, sort._, facet._, terms._
import scala.collection._, JavaConversions._
import scalaz._, Scalaz._

trait Searching extends Query with Search with Multisearch with Percolate {
  self: Indexer =>
}

trait Query {
  self: Indexer =>
  def query(queryString: String) = query_send(queryString).actionGet
  def query_send(queryString: String) = query_prepare(queryString).execute
  def query_prepare(queryString: String) = client.prepareSearch().setQuery(queryString)
}

trait Search {
  self: Indexer =>

  type ScriptField = Tuple3[String, String, Map[String, Object]]
  type PartialField = Tuple3[String, String, String]

  def search(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil, 
    filter: Map[String, Object] = Map(),
    query: QueryBuilder = matchAllQuery,
    fields: Iterable[String] = Nil,
    scriptFields: Iterable[ScriptField] = Nil,
    partialFields: Iterable[PartialField] = Nil,
    facets: Iterable[TermsFacetBuilder] = Nil,
    sorting: Map[String, SortOrder] = Map(),
    from: Option[Int] = None, 
    size: Option[Int] = None,
    searchType: Option[SearchType] = None, 
    explain: Option[Boolean] = None) = {
    search_send(indices, types, query, filter, fields, scriptFields, partialFields, facets, sorting, from, size, searchType, explain).actionGet
  }

  def search_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery, 
    filter: Map[String, Object] = Map(),
    fields: Iterable[String] = Nil,
    scriptFields: Iterable[ScriptField] = Nil,
    partialFields: Iterable[PartialField] = Nil,
    facets: Iterable[TermsFacetBuilder] = Nil,
    sorting: Map[String, SortOrder] = Map(),
    from: Option[Int] = None, 
    size: Option[Int] = None,
    searchType: Option[SearchType] = None, 
    explain: Option[Boolean] = None) = {
    search_prepare(indices, types, query, filter, fields, scriptFields, partialFields, facets, sorting, from, size, searchType, explain).execute
  }

  def search_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery, 
    filter: Map[String, Object] = Map(),
    fields: Iterable[String] = Nil,
    scriptFields: Iterable[ScriptField] = Nil,
    partialFields: Iterable[PartialField] = Nil,
    //todo: need to allow for includes/excludes, such as: partialFields: Iterable[Tuple3[String, Iterable[String], Iterable[String]]] = Nil,
    facets: Iterable[TermsFacetBuilder] = Nil,
    sorting: Map[String, SortOrder] = Map(),
    from: Option[Int] = None, 
    size: Option[Int] = None,
    searchType: Option[SearchType] = None, 
    explain: Option[Boolean] = None) = {
		  /* method body */
    val request = client.prepareSearch(indices.toArray: _*)
    request.setTypes(types.toArray: _*)
    request.setQuery(query)
    if (!filter.isEmpty) request.setFilter(filter)
    for (each <- fields) request.addField(each)
    for ((field, script, parameters) <- scriptFields) 
      if (parameters == null) request.addScriptField(field, script)
      else request.addScriptField(field, script, parameters)
    for ((name, include, exclude) <- partialFields) request.addPartialField(name, include, exclude)
    for (each <- facets) request.addFacet(each)
    for ((field, order) <- sorting) request.addSort(field, order)
    from some { request.setFrom(_) }
    size some { request.setSize(_) }
    searchType some { request.setSearchType(_) }
    explain some { request.setExplain(_) }
    request
  }
}

trait Multisearch {
  self: Indexer =>

  def multisearch(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) =
    multisearch_send(queries = queries).actionGet

  def multisearch_send(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) =
    multisearch_prepare(queries = queries).execute

  def multisearch_prepare(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) = {
    val request = client.prepareMultiSearch
    for (each <- queries) request.add(search_prepare(query = each))
    request
  }
}

trait Percolate {
  self: Indexer =>

  def percolate(index: String, `type`: String, docBuilder: XContentBuilder) =
    percolate_send(index, `type`, docBuilder).actionGet

  def percolate_send(index: String, `type`: String, docBuilder: XContentBuilder) =
    percolate_prepare(index, `type`, docBuilder).execute

  def percolate_prepare(index: String, `type`: String, docBuilder: XContentBuilder) = {
    client
      .preparePercolate(index, `type`)
      .setSource(docBuilder)
  }
}
