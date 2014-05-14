package scalastic.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.action.support.broadcast._
import org.elasticsearch.index.query._, QueryBuilders._
import org.elasticsearch.search._, facet._, sort._, SortBuilders._, builder._
import scala.collection._, JavaConversions._
import org.elasticsearch.common.geo._

trait Searching 
  extends Query 
  with Search 
  with SearchScroll with ClearScroll
  with MoreLikeThis
  with Multisearch 
  with Percolate 
  with ValidateQuery {
  self: Indexer =>
}

trait Query {
  self: Indexer =>
  def query(queryString: String) = query_send(queryString).actionGet
  def query_send(queryString: String) = query_prepare(queryString).execute
  def query_prepare(queryString: String) = client.prepareSearch().setQuery(queryString)
}

object SearchParameterTypes {
  import org.elasticsearch.common.unit._

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

  case class HighlightField(
    name: String,
    fragmentSize: Int = -1,
    fragmentOffset: Int = -1,
    numOfFragments: Int = -1,
    requireFieldMatch: Option[Boolean] = None // currently ignored in the Java API
  )

  case class Highlight(
      fields: Iterable[HighlightField] = Nil,
      order: Option[String] = None,
      requireFieldMatch: Option[Boolean] = None,
      encoder: Option[String] = None,
      preTags: Iterable[String] = Nil,
      postTags: Iterable[String] = Nil,
      tagsSchema: Option[String] = None) {

    def setIn(request: SearchRequestBuilder) {
      fields foreach { f =>
        f match {
          case HighlightField(name, -1, -1, -1, None) => request.addHighlightedField(name)
          case HighlightField(name, fs, -1, -1, None) => request.addHighlightedField(name, fs)
          case HighlightField(name, fs, fo, -1, None) => request.addHighlightedField(name, fs, fo)
          case HighlightField(name, fs, fo, nf, None) => request.addHighlightedField(name, fs, fo, nf)
          case HighlightField(name, fs, fo, nf, Some(r)) => request.addHighlightedField(name, fs, fo, nf)
        }
      }
      order foreach { request.setHighlighterOrder(_) }
      requireFieldMatch foreach { request.setHighlighterRequireFieldMatch(_) }
      encoder foreach { request.setHighlighterEncoder(_) }
      if (!preTags.isEmpty) request.setHighlighterPreTags(preTags.toArray: _*)
      if (!postTags.isEmpty) request.setHighlighterPostTags(postTags.toArray: _*)
      tagsSchema foreach { request.setHighlighterTagsSchema(_) }
    }
  }
}

trait SearchScroll {
  self: Indexer =>

  def searchScroll(
    scrollId: String,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    scroll: Option[String] = None) = searchScroll_send(scrollId, listenerThreaded, operationThreading, scroll).actionGet

  def searchScroll_send(
    scrollId: String,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    scroll: Option[String] = None) = searchScroll_prepare(scrollId, listenerThreaded, operationThreading, scroll).execute

  def searchScroll_prepare(
    scrollId: String,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    scroll: Option[String] = None) = {
      /* method body */
    val request = client.prepareSearchScroll(scrollId)
    listenerThreaded foreach { request.listenerThreaded(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    scroll foreach { request.setScroll(_) }
    request
  }
}

trait ClearScroll {
  self: Indexer =>

  def clearScroll(cursorIds: Iterable[String]) = clearScroll_send(cursorIds).actionGet

  def clearScroll_send(cursorIds: Iterable[String]) = clearScroll_prepare(cursorIds).execute

  def clearScroll_prepare(cursorIds: Iterable[String]) = {
      /* method body */
    val request = client.prepareClearScroll()
    request.setScrollIds(cursorIds.toList)
    request
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
    facets: Iterable[FacetBuilder] = Nil,
    fields: Iterable[String] = Nil,
    filter: Option[FilterBuilder] = None,
    from: Option[Int] = None,
    highlight: Highlight = Highlight(),
    indexBoosts: Map[String, Float] = Map(),
    internalBuilder: Option[SearchSourceBuilder] = None,
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[PartialField] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[String] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sortings: Iterable[Sorting] = Nil,
    source: Option[String] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) = search_send(indices, types, query, explain, extraSource, facets, fields, filter, from, highlight, indexBoosts, internalBuilder, minScore, operationThreading, partialFields, preference, queryHint, routing, scriptFields, scroll, searchType, size, sortings, source, statsGroups, timeout, trackScores).actionGet

  def search_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    /* the rest ... */
    explain: Option[Boolean] = None,
    extraSource: Option[Map[String, Object]] = None,
    facets: Iterable[FacetBuilder] = Nil,
    fields: Iterable[String] = Nil,
    filter: Option[FilterBuilder] = None,
    from: Option[Int] = None,
    highlight: Highlight = Highlight(),
    indexBoosts: Map[String, Float] = Map(),
    internalBuilder: Option[SearchSourceBuilder] = None,
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[PartialField] = Nil,
    preference: Option[String] = None,
    queryHint: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[String] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sortings: Iterable[Sorting] = Nil,
    source: Option[String] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None) = search_prepare(indices, types, query, explain, extraSource, facets, fields, filter, from, highlight, indexBoosts, internalBuilder, minScore, operationThreading, partialFields, preference, routing, scriptFields, scroll, searchType, size, sortings, source, statsGroups, timeout, trackScores).execute

  def search_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    /* the rest ... */
    explain: Option[Boolean] = None,
    extraSource: Option[Map[String, Object]] = None,
    facets: Iterable[FacetBuilder] = Nil,
    fields: Iterable[String] = Nil,
    filter: Option[FilterBuilder] = None,
    from: Option[Int] = None,
    highlight: Highlight = Highlight(),
    indexBoosts: Map[String, Float] = Map(),
    internalBuilder: Option[SearchSourceBuilder] = None,
    minScore: Option[Float] = None,
    operationThreading: Option[SearchOperationThreading] = None,
    partialFields: Iterable[PartialField] = Nil,
    preference: Option[String] = None,
    routing: Option[String] = None,
    scriptFields: Iterable[ScriptField] = Nil,
    scroll: Option[String] = None,
    searchType: Option[SearchType] = None,
    size: Option[Int] = None,
    sortings: Iterable[Sorting] = Nil,
    source: Option[String] = None,
    statsGroups: Iterable[String] = Nil,
    timeout: Option[String] = None,
    trackScores: Option[Boolean] = None,
    highlighterQuery: Option[QueryBuilder] = None,
    highlighterNoMatchSize: Option[Int] = None) = {
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
    filter foreach { request.setPostFilter(_) }
    from foreach { request.setFrom(_) }
    highlight.setIn(request)
    indexBoosts foreach { case (key, value) => request.addIndexBoost(key, value) }
    internalBuilder foreach { request.internalBuilder(_) }
    minScore foreach { request.setMinScore(_) }
    operationThreading foreach { request.setOperationThreading(_) }
    partialFields foreach { each => request.addPartialField(each.name, each.includes.toArray, each.excludes.toArray) }
    preference foreach { request.setPreference(_) }
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
    highlighterQuery foreach { request.setHighlighterQuery(_) }
    highlighterNoMatchSize foreach { request.setHighlighterNoMatchSize(_) }
    request
  }
}

trait Multisearch {
  self: Indexer =>

  def multisearch(requests: Iterable[SearchRequestBuilder] = Seq(search_prepare())) = multisearch_send(requests = requests).actionGet
  def multisearch_send(requests: Iterable[SearchRequestBuilder] = Seq(search_prepare())) = multisearch_prepare(requests = requests).execute
  def multisearch_prepare(requests: Iterable[SearchRequestBuilder] = Seq(search_prepare())) = {
    val request = client.prepareMultiSearch
    for (each <- requests) request.add(each)
    request
  }

  def multisearchByQuery(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) = multisearchByQuery_send(queries = queries).actionGet
  def multisearchByQuery_send(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) = multisearchByQuery_prepare(queries = queries).execute
  def multisearchByQuery_prepare(queries: Iterable[QueryBuilder] = Seq(matchAllQuery)) = multisearch_prepare(queries map (each => search_prepare(query = each)))
}

trait MoreLikeThis {
  self: Indexer =>

  def moreLikeThis(
    index: String,
    `type`: String,
    id: String,
    boostTerms: Option[Float] = None,
    fields: Iterable[String] = Nil,
    maxDocFreq: Option[Int] = None,
    maxQueryTerms: Option[Int] = None,
    maxWordLen: Option[Int] = None,
    minDocFreq: Option[Int] = None,
    minTermFreq: Option[Int] = None,
    minWordLen: Option[Int] = None,
    percentTermsToMatch: Option[Float] = None,
    from: Option[Int] = None,
    searchIndices: Iterable[String] = Nil,
    searchScroll: Option[Scroll] = None,
    searchSize: Option[Int] = None,
    searchSource: Option[Map[String, Object]] = None,
    searchType: Option[SearchType] = None,
    searchTypes: Iterable[String] = Nil,
    stopwords: Iterable[String] = Nil) = moreLikeThis_send(index, `type`, id, boostTerms, fields, maxDocFreq, maxQueryTerms, maxWordLen, minDocFreq, minTermFreq, minWordLen, percentTermsToMatch, from, searchIndices, searchScroll, searchSize, searchSource, searchType, searchTypes, stopwords).actionGet

  def moreLikeThis_send(
    index: String,
    `type`: String,
    id: String,
    boostTerms: Option[Float] = None,
    fields: Iterable[String] = Nil,
    maxDocFreq: Option[Int] = None,
    maxQueryTerms: Option[Int] = None,
    maxWordLen: Option[Int] = None,
    minDocFreq: Option[Int] = None,
    minTermFreq: Option[Int] = None,
    minWordLen: Option[Int] = None,
    percentTermsToMatch: Option[Float] = None,
    from: Option[Int] = None,
    searchIndices: Iterable[String] = Nil,
    searchScroll: Option[Scroll] = None,
    searchSize: Option[Int] = None,
    searchSource: Option[Map[String, Object]] = None,
    searchType: Option[SearchType] = None,
    searchTypes: Iterable[String] = Nil,
    stopwords: Iterable[String] = Nil) = moreLikeThis_prepare(index, `type`, id, boostTerms, fields, maxDocFreq, maxQueryTerms, maxWordLen, minDocFreq, minTermFreq, minWordLen, percentTermsToMatch, from, searchIndices, searchScroll, searchSize, searchSource, searchType, searchTypes, stopwords).execute

  def moreLikeThis_prepare(
    index: String,
    `type`: String,
    id: String,
    boostTerms: Option[Float] = None,
    fields: Iterable[String] = Nil,
    maxDocFreq: Option[Int] = None,
    maxQueryTerms: Option[Int] = None,
    maxWordLen: Option[Int] = None,
    minDocFreq: Option[Int] = None,
    minTermFreq: Option[Int] = None,
    minWordLen: Option[Int] = None,
    percentTermsToMatch: Option[Float] = None,
    from: Option[Int] = None,
    searchIndices: Iterable[String] = Nil,
    searchScroll: Option[Scroll] = None,
    searchSize: Option[Int] = None,
    searchSource: Option[Map[String, Object]] = None,
    searchType: Option[SearchType] = None,
    searchTypes: Iterable[String] = Nil,
    stopwords: Iterable[String] = Nil) = {
      /* method body */
    val request = client.prepareMoreLikeThis(index, `type`, id)
    boostTerms foreach { request.setBoostTerms(_) }
    if (!fields.isEmpty) request.setField(fields.toArray: _*)
    maxDocFreq foreach { request.setMaxDocFreq(_) }
    maxQueryTerms foreach { request.maxQueryTerms(_) }
    maxWordLen foreach { request.setMaxWordLen(_) }
    minDocFreq foreach { request.setMinDocFreq(_) }
    minTermFreq foreach { request.setMinTermFreq(_) }
    minWordLen foreach { request.setMinWordLen(_) }
    percentTermsToMatch foreach { request.setPercentTermsToMatch(_) }
    from foreach { request.setSearchFrom(_) }
    if (!searchIndices.isEmpty) request.setSearchIndices(searchIndices.toArray: _*)
    searchScroll foreach { request.setSearchScroll(_) }
    searchSize foreach { request.setSearchSize(_) }
    searchSource foreach { request.setSearchSource(_) }
    searchType foreach { request.setSearchType(_) }
    if (!searchTypes.isEmpty) request.setSearchTypes(searchTypes.toArray: _*)
    if (!stopwords.isEmpty) request.setStopWords(stopwords.toArray: _*)
    request
  }
}

trait Percolate {
  self: Indexer =>

  def percolate(
    index: String,
    `type`: String,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[String] = None,
    preferLocal: Option[String] = None,
    source: Option[String] = None) = percolate_send(index, `type`, listenerThreaded, operationThreaded, preferLocal, source).actionGet

  def percolate_send(
    index: String,
    `type`: String,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[String] = None,
    preferLocal: Option[String] = None,
    source: Option[String] = None) = percolate_prepare(index, `type`, listenerThreaded, operationThreaded, preferLocal, source).execute

  def percolate_prepare(
    index: String,
    `type`: String,
    listenerThreaded: Option[Boolean] = None,
    operationThreaded: Option[String] = None,
    preferLocal: Option[String] = None,
    source: Option[String] = None) = {
      /* method body */
    val request = client.preparePercolate
    request.setIndices(index)
    request.setDocumentType(`type`)

    listenerThreaded foreach { request.setListenerThreaded(_) }
    operationThreaded foreach { request.setOperationThreading(_) }
    
    // local, primary, or custom value (changed from setPreferLocal in 1.0+)
    preferLocal foreach { request.setPreference(_) }
    
    source foreach { request.setSource(_) }
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
    operationThreading: Option[String] = None) = validateQuery_send(indices, types, query, explain, listenerThreaded, operationThreading).actionGet

  def validateQuery_send(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    explain: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[String] = None) = validateQuery_prepare(indices, types, query, explain, listenerThreaded, operationThreading).execute

  def validateQuery_prepare(
    indices: Iterable[String] = Nil,
    types: Iterable[String] = Nil,
    query: QueryBuilder = matchAllQuery,
    explain: Option[Boolean] = None,
    listenerThreaded: Option[Boolean] = None,
    operationThreading: Option[String] = None) = {
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
