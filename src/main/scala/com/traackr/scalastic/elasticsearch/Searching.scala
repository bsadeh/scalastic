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
  import highlight.HighlightBuilder.{ Field => HighlightField }

  def search(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    /* the rest ... */
    explain: Option[Boolean] = None,
    extraSource: Option[Map[String, Object]] = None,
    facets: Iterable[AbstractFacetBuilder] = Nil,
    fields: Iterable[String] = Nil,
    filter: Option[Map[String, Object]] = None,
    from: Option[Int] = None,
    highlighterEncoder: Option[String] = None,
    highLightFields: Iterable[HighlightField] = Nil,
    highlighterOrder: Option[String] = None,
    highlighterPostTags: Iterable[String] = Nil,
    highlighterPreTags: Iterable[String] = Nil,
    highlighterRequireFieldMatch: Option[Boolean] = None,
    highlighterTagsSchema: Option[String] = None,
    indexBoosts: Map[String, Float] = Map(),
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[FieldIncludesExcludes] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[Scroll] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sorting: Iterable[SortSpec] = Nil,
    source: Option[Map[String, Object]] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) = {
    search_send(indices, types, query, explain, extraSource, facets, fields, filter, from, highlighterEncoder, highLightFields, highlighterOrder, highlighterPostTags, highlighterPreTags, highlighterRequireFieldMatch, highlighterTagsSchema, indexBoosts, minScore, operationThreading, partialFields, preference, queryHint, routing, scriptFields, scroll, searchType, size, sorting, source, statsGroups, timeout, trackScores).actionGet
  }

  def search_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    /* the rest ... */
    explain: Option[Boolean] = None,
    extraSource: Option[Map[String, Object]] = None,
    facets: Iterable[AbstractFacetBuilder] = Nil,
    fields: Iterable[String] = Nil,
    filter: Option[Map[String, Object]] = None,
    from: Option[Int] = None,
    highlighterEncoder: Option[String] = None,
    highLightFields: Iterable[HighlightField] = Nil,
    highlighterOrder: Option[String] = None,
    highlighterPostTags: Iterable[String] = Nil,
    highlighterPreTags: Iterable[String] = Nil,
    highlighterRequireFieldMatch: Option[Boolean] = None,
    highlighterTagsSchema: Option[String] = None,
    indexBoosts: Map[String, Float] = Map(),
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[FieldIncludesExcludes] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[Scroll] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sorting: Iterable[SortSpec] = Nil,
    source: Option[Map[String, Object]] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) = {
    search_prepare(indices, types, query, explain, extraSource, facets, fields, filter, from, highlighterEncoder, highLightFields, highlighterOrder, highlighterPostTags, highlighterPreTags, highlighterRequireFieldMatch, highlighterTagsSchema, indexBoosts, minScore, operationThreading, partialFields, preference, queryHint, routing, scriptFields, scroll, searchType, size, sorting, source, statsGroups, timeout, trackScores).execute
  }

  def search_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    /* the rest ... */
    explain: Option[Boolean] = None,
    extraSource: Option[Map[String, Object]] = None,
    facets: Iterable[AbstractFacetBuilder] = Nil,
    fields: Iterable[String] = Nil,
    filter: Option[Map[String, Object]] = None,
    from: Option[Int] = None,
    highlighterEncoder: Option[String] = None,
    highLightFields: Iterable[HighlightField] = Nil,
    highlighterOrder: Option[String] = None,
    highlighterPostTags: Iterable[String] = Nil,
    highlighterPreTags: Iterable[String] = Nil,
    highlighterRequireFieldMatch: Option[Boolean] = None,
    highlighterTagsSchema: Option[String] = None,
    indexBoosts: Map[String, Float] = Map(),
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[FieldIncludesExcludes] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[Scroll] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sorting: Iterable[SortSpec] = Nil,
    source: Option[Map[String, Object]] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) = {

    /* method body */
    val request = client.prepareSearch(indices.toArray: _*)
    request.setTypes(types.toArray: _*)
    request.setQuery(query)

    explain foreach { request.setExplain(_) }
    extraSource foreach { request.setExtraSource(_) }
    facets foreach { request.addFacet(_) }
    fields foreach { request.addField(_) }
    filter foreach { request.setFilter(_) }
    from foreach { request.setFrom(_) }
    highlighterEncoder foreach { request.setHighlighterEncoder(_) }
    highlighterOrder foreach { request.setHighlighterOrder(_) }
    highLightFields foreach { request.addHighlightedField(_) }
    if (!highlighterPostTags.isEmpty) request.setHighlighterPostTags(highlighterPostTags.toArray: _*)
    if (!highlighterPreTags.isEmpty) request.setHighlighterPreTags(highlighterPreTags.toArray: _*)
    indexBoosts foreach { case (key, value) => request.addIndexBoost(key, value) }
    minScore foreach { request.setMinScore(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    partialFields foreach { case (name, includes, excludes) => request.addPartialField(name, includes.toArray, excludes.toArray) }
    preference foreach { request.setPreference(_) }
    queryHint foreach { request.setQueryHint(_) }
    routing foreach { request.setRouting(_) }
    scriptFields foreach {
      case (field, script, null) => request.addScriptField(field, script)
      case (field, script, parameters) => request.addScriptField(field, script, parameters)
    }
    scroll foreach { request.setScroll(_) }
    searchType foreach { request.setSearchType(_) }
    size foreach { request.setSize(_) }
    sorting foreach { case (field, order) => request.addSort(field, order) }
    source foreach { request.setSource(_) }
    if (!statsGroups.isEmpty) request.setStats(statsGroups.toArray: _*)
    timeout foreach { request.setTimeout(_) }
    trackScores foreach { request.setTrackScores(_) }
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
