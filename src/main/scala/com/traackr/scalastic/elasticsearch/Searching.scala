package com.traackr.scalastic.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.action.support.broadcast._
import org.elasticsearch.index.query._, QueryBuilders._
import org.elasticsearch.common.xcontent._
import org.elasticsearch.search._, facet._, terms._, sort._, SortBuilders._
import scala.collection._, JavaConversions._

trait Searching extends Query with Search with Multisearch with Percolate with ValidateQuery {
  self: Indexer =>
}

trait Query {
  self: Indexer =>
  def query(queryString: String) = query_send(queryString).actionGet
  def query_send(queryString: String) = query_prepare(queryString).execute
  def query_prepare(queryString: String) = client.prepareSearch().setQuery(queryString)
}

object SearchParameterTypes {
  import org.elasticsearch.index.mapper.geo._
  import org.elasticsearch.index.search.geo._
  import org.elasticsearch.common.unit._
  import highlight.HighlightBuilder.{ Field => HighlightField }

  case class ScriptField(name: String, script: String, parameters: Map[String, Object] = Map(), lang: Option[String] = None)

  case class PartialField(name: String, includes: Iterable[String], excludes: Iterable[String]) {
    def this(name: String, include: Option[String], exclude: Option[String]) = this(name, include.toIterable, exclude.toIterable)
  }

  sealed abstract class Sorting(order: SortOrder) {
    def newBuilder: SortBuilder
    def toBuilder = newBuilder.order(order)
  }
  case class FieldSort(field: String, ignoreUnmapped: Option[Boolean] = None, missing: Option[Any] = None, order: SortOrder = SortOrder.ASC) extends Sorting(order) {
    def newBuilder = {
      val builder = fieldSort(field)
      ignoreUnmapped.foreach { builder.ignoreUnmapped(_) }
      missing.foreach { builder.missing(_) }
      builder
    }
  }
  case class ScoreSort(order: SortOrder = SortOrder.ASC) extends Sorting(order) {
    def newBuilder = scoreSort
  }
  case class ScriptSort(script: String, `type`: String, lang: Option[String] = None, parameters: Map[String, Object] = Map(), order: SortOrder = SortOrder.ASC) extends Sorting(order) {
    def newBuilder = {
      val builder = scriptSort(script, `type`)
      lang foreach { builder.lang(_) }
      parameters foreach { case (key, value) => builder.param(key, value) }
      builder
    }
  }
  case class GeoDistanceSort(field: String, geoDistance: Option[GeoDistance] = None, geohash: Option[String] = None, geoPoint: Option[GeoPoint] = None, unit: Option[DistanceUnit] = None, order: SortOrder = SortOrder.ASC) extends Sorting(order) {
    def newBuilder = {
      val builder = geoDistanceSort(field)
      geoDistance foreach { builder.geoDistance(_) }
      geohash foreach { builder.geohash(_) }
      geoPoint foreach { each => builder.point(each.lat, each.lon) }
      unit foreach { builder.unit(_) }
      builder
    }
  }

  case class Highlight(
      fields: Iterable[HighlightField] = Nil,
      order: Option[String] = None,
      requireFieldMatch: Option[Boolean] = None,
      encoder: Option[String] = None,
      preTags: Iterable[String] = Nil,
      postTags: Iterable[String] = Nil,
      tagsSchema: Option[String] = None) {

    def setIn(request: SearchRequestBuilder) {
      fields foreach { request.addHighlightedField(_) }
      order foreach { request.setHighlighterOrder(_) }
      requireFieldMatch foreach { request.setHighlighterRequireFieldMatch(_) }
      encoder foreach { request.setHighlighterEncoder(_) }
      if (!preTags.isEmpty) request.setHighlighterPreTags(preTags.toArray: _*)
      if (!postTags.isEmpty) request.setHighlighterPostTags(postTags.toArray: _*)
      tagsSchema foreach { request.setHighlighterTagsSchema(_) }
    }
  }
}

trait Search {
  self: Indexer =>

  import SearchParameterTypes._

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
    highlight: Highlight = Highlight(),
    indexBoosts: Map[String, Float] = Map(),
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[PartialField] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[Scroll] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sortings: Iterable[Sorting] = Nil,
    source: Option[Map[String, Object]] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) =
    search_send(indices, types, query, explain, extraSource, facets, fields, filter, from, highlight, indexBoosts, minScore, operationThreading, partialFields, preference, queryHint, routing, scriptFields, scroll, searchType, size, sortings, source, statsGroups, timeout, trackScores).actionGet

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
    highlight: Highlight = Highlight(),
    indexBoosts: Map[String, Float] = Map(),
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[PartialField] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[Scroll] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sortings: Iterable[Sorting] = Nil,
    source: Option[Map[String, Object]] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) =
    search_prepare(indices, types, query, explain, extraSource, facets, fields, filter, from, highlight, indexBoosts, minScore, operationThreading, partialFields, preference, queryHint, routing, scriptFields, scroll, searchType, size, sortings, source, statsGroups, timeout, trackScores).execute

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
    highlight: Highlight = Highlight(),
    indexBoosts: Map[String, Float] = Map(),
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[PartialField] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[Scroll] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sortings: Iterable[Sorting] = Nil,
    source: Option[Map[String, Object]] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) = {
		  /* method body */
    // ... essentials
    val request = client.prepareSearch(indices.toArray: _*)
    request.setTypes(types.toArray: _*)
    request.setQuery(query)
    // ... and the rest
    explain foreach { request.setExplain(_) }
    extraSource foreach { request.setExtraSource(_) }
    facets foreach { request.addFacet(_) }
    fields foreach { request.addField(_) }
    filter foreach { request.setFilter(_) }
    from foreach { request.setFrom(_) }
    highlight.setIn(request)
    indexBoosts foreach { case (key, value) => request.addIndexBoost(key, value) }
    minScore foreach { request.setMinScore(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    partialFields foreach { each => request.addPartialField(each.name, each.includes.toArray, each.excludes.toArray) }
    preference foreach { request.setPreference(_) }
    queryHint foreach { request.setQueryHint(_) }
    routing foreach { request.setRouting(_) }
    scriptFields foreach { each => request.addScriptField(each.name, each.lang getOrElse (null), each.script, each.parameters) }
    scroll foreach { request.setScroll(_) }
    searchType foreach { request.setSearchType(_) }
    size foreach { request.setSize(_) }
    sortings foreach { each => request.addSort(each.toBuilder) }
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

  def percolate(
    index: String, `type`: String,
    source: Option[Map[String, Object]] = None,
    operationThreaded: Option[Boolean] = None,
    preferLocal: Option[Boolean] = None) =
    percolate_send(index, `type`, source, operationThreaded, preferLocal).actionGet

  def percolate_send(
    index: String, `type`: String,
    source: Option[Map[String, Object]] = None,
    operationThreaded: Option[Boolean] = None,
    preferLocal: Option[Boolean] = None) =
    percolate_prepare(index, `type`, source, operationThreaded, preferLocal).execute

  def percolate_prepare(
    index: String, `type`: String,
    source: Option[Map[String, Object]] = None,
    operationThreaded: Option[Boolean] = None,
    preferLocal: Option[Boolean] = None) = {
    val request = client.preparePercolate(index, `type`)
    source foreach { request.setSource(_) }
    operationThreaded foreach { request.setOperationThreaded(_) }
    preferLocal foreach { request.setPreferLocal(_) }
    request
  }
}

trait ValidateQuery {
  self: Indexer =>

  def validateQuery(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    explain: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None) =
    validateQuery_send(indices, types, query, explain, listenerThreaded, operationThreading).actionGet

  def validateQuery_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    explain: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None) =
    validateQuery_prepare(indices, types, query, explain, listenerThreaded, operationThreading).execute

  def validateQuery_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    explain: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[BroadcastOperationThreading] = None) = {
		  /* method body */
    val request = client.admin.indices.prepareValidateQuery(indices.toArray: _*)
    request.setTypes(types.toArray: _*)
    request.setQuery(query)
    explain foreach { request.setExplain(_) }
    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    request
  }
}
