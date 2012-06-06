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
  type FieldIncludesExcludes = Tuple3[String, Iterable[String], Iterable[String]]
  type SortSpec = Tuple2[String, SortOrder]

  def search(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    filter: Map[String, Object] = Map(),
    query: QueryBuilder = matchAllQuery,
    fields: Iterable[String] = Nil,
    scriptFields: Iterable[ScriptField] = Nil,
    partialFields: Iterable[FieldIncludesExcludes] = Nil,
    facets: Iterable[AbstractFacetBuilder] = Nil,
    sorting: Iterable[SortSpec] = Nil,
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
    partialFields: Iterable[FieldIncludesExcludes] = Nil,
    facets: Iterable[AbstractFacetBuilder] = Nil,
    sorting: Iterable[SortSpec] = Nil,
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
    partialFields: Iterable[FieldIncludesExcludes] = Nil,
    facets: Iterable[AbstractFacetBuilder] = Nil,
    sorting: Iterable[SortSpec] = Nil,
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
    for ((name, includes, excludes) <- partialFields) request.addPartialField(name, includes.toArray, excludes.toArray)
    for (each <- facets) request.addFacet(each)
    for ((field, order) <- sorting) request.addSort(field, order)
    from foreach { request.setFrom(_) }
    size foreach { request.setSize(_) }
    searchType foreach { request.setSearchType(_) }
    explain foreach { request.setExplain(_) }
    request
  }
}

trait Multisearch {
  self: Indexer =>

  def multisearch(requests: Iterable[SearchRequestBuilder] = Seq(search_prepare())) =
    multisearch_send(requests = requests).actionGet

  def multisearch_send(requests: Iterable[SearchRequestBuilder] = Seq(search_prepare())) =
    multisearch_prepare(requests = requests).execute

  def multisearch_prepare(requests: Iterable[SearchRequestBuilder] = Seq(search_prepare())) = {
    val request = client.prepareMultiSearch
    for (each <- requests) request.add(each)
    request
  }

  def multisearchByQuery(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) =
    multisearchByQuery_send(queries = queries).actionGet

  def multisearchByQuery_send(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) =
    multisearchByQuery_prepare(queries = queries).execute

  def multisearchByQuery_prepare(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) =
    multisearch_prepare(queries map (each => search_prepare(query = each)))
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
