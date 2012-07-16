package org.elasticsearch.test.integration.search.facet

import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.action.search._
import java.util.concurrent._
import org.elasticsearch.common.unit._
import org.elasticsearch.search.facet.FacetBuilders._
import org.elasticsearch.search.facet.datehistogram._
import org.elasticsearch.search.facet.filter._
import org.elasticsearch.search.facet.histogram._
import org.elasticsearch.search.facet.query._
import org.elasticsearch.search.facet.range._
import org.elasticsearch.search.facet.statistical._
import org.elasticsearch.search.facet.terms._, bytes._, doubles._, ints._, longs._, shorts._
import org.elasticsearch.search.facet.termsstats._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SimpleFacetsTests extends IndexerBasedTest {
	
  override def defaultSettings = Map("number_of_shards" -> "%s".format(numberOfShards()), "number_of_replicas" -> "0")

  protected def numberOfShards(): Int = 1

  protected def numberOfNodes(): Int = 1

  protected def numberOfRuns(): Int = 5

  test("testBinaryFacet") {
    indexer.index(indexName, "type1", null, """{"tag": "green"}""")
    indexer.index(indexName, "type1", null, """{"tag": "blue"}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search_prepare(searchType=Some(SearchType.COUNT))
        .setFacets("""{"facet1": {"terms": {"field": "tag"}}}""".getBytes)
        .execute.actionGet
      response.hits.totalHits should be === (2)
      response.hits.hits.length should be === (0)
      val facet: TermsFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      Set("green", "blue") should contain (facet.entries().get(0).term)
      facet.entries().get(0).count() should be === (1)
      Set("green", "blue") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
    }
  }

  test("testSearchFilter") {
    indexer.index(indexName, "type1", null, """{"tag": "green"}""")
    indexer.index(indexName, "type1", null, """{"tag": "blue"}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      var response = indexer.search(facets = Seq(termsFacet("facet1").field("tag").size(10)))
      response.hits.hits.length should be === (2)
      var facet: TermsFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      Set("green", "blue") should contain (facet.entries().get(0).term)
      facet.entries().get(0).count() should be === (1)
      Set("green", "blue") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
      response = indexer.search(filter = Some(termFilter("tag", "blue")), facets = Seq(termsFacet("facet1").field("tag").size(10)))
      response.hits.hits.length should be === (1)
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      Set("green", "blue") should contain (facet.entries().get(0).term)
      facet.entries().get(0).count() should be === (1)
      Set("green", "blue") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
    }
  }

  test("testFacetsWithSize0") {
    indexer.index(indexName, "type1", null, """{"stag": "111", "lstag": 111, "tag": ["xxx", "yyy"], "ltag": [1000, 2000]}""")
    indexer.index(indexName, "type1", null, """{"stag": "111", "lstag": 111, "tag": ["zzz", "yyy"], "ltag": [3000, 2000]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      var response = indexer.search(size=Some(0), query=termQuery("stag", "111"), facets=Seq(termsFacet("facet1").field("stag").size(10)))
      response.hits.hits.length should be === (0)
      var facet: TermsFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      response = indexer.search(searchType=Some(SearchType.QUERY_AND_FETCH), size=Some(0), query=termQuery("stag", "111"), facets=Seq(
        termsFacet("facet1").field("stag").size(10),
        termsFacet("facet2").field("tag").size(10)))
      response.hits.hits.length should be === (0)
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
    }
  }

  test("testTermsIndexFacet") {
    pending //fixme: failing test
    indexer.createIndex("test1")
    indexer.createIndex("test2")
    indexer.index("test1", "type1", null, """{"stag": "111"}""")
    indexer.index("test1", "type1", null, """{"stag": "111"}""")
    indexer.index("test2", "type1", null, """{"stag": "111"}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(size=Some(0), facets=Seq(termsFacet("facet1").field("_index").size(10)))
      var facet: TermsFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("test1")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).term() should be === ("test2")
      facet.entries().get(1).count() should be === (1)
    }
    try {
      indexer.deleteIndex(Seq("test1"))
      indexer.deleteIndex(Seq("test2"))
    } catch {
      case e: Exception =>
    }
  }

  test("testFilterFacets") {
    indexer.index(indexName, "type1", null, """{"stag": "111", "tag": ["xxx", "yyy"]}""")
    indexer.index(indexName, "type1", null, """{"stag": "111", "tag": ["zzz", "yyy"]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(
        filterFacet("facet1").filter(termFilter("stag", "111")),
        filterFacet("facet2").filter(termFilter("tag", "xxx")),
        filterFacet("facet3").filter(termFilter("tag", "yyy"))))
      var facet: FilterFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.count() should be === (2)
    }
  }

  test("testTermsFacetsMissing") {
    indexer.putMapping(indexName, "type1", """{"type1": {"properties": {"bstag": {"type": "byte"}, "shstag": {"type": "short"}, "istag": {"type": "integer"}, "lstag": {"type": "long"}, "fstag": {"type": "float"}, "dstag": {"type": "double"}}}}""")
    indexer.index(indexName, "type1", null, """{"stag": "111", "bstag": 111, "shtag": 111, "lstag": 111, "fstag": 111.1, "dstag": 111.1}""")
    indexer.index(indexName, "type1", null, """{"kuku": "kuku"}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets = Seq(termsFacet("facet1").field("stag").size(10)))
      var facet: TermsFacet = response.facets().facet("facet1")
      facet.missingCount() should be === (1)
    }
  }

  test("testTermsFacetsNoHint") {
    testTermsFacets(null)
  }

  test("testTermsFacetsMapHint") {
    testTermsFacets("map")
  }

  private def testTermsFacets(executionHint: String) {
    pending //fixme: failing test
    indexer.putMapping(indexName, "type1", """{"type1": {"properties": {"bstag": {"type": "byte"}, "shstag": {"type": "short"}, "istag": {"type": "integer"}, "lstag": {"type": "long"}, "fstag": {"type": "float"}, "dstag": {"type": "double"}}}}""")
    indexer.index(indexName, "type1", null, """{"stag": "111", "bstag": 111, "shtag": 111, "lstag": 111, "fstag": 111.1, "dstag": 111.1, "tag": ["xxx", "yyy"], "ltag": [1000, 2000], "dtag": [1000.1, 2000.1]}""")
    indexer.index(indexName, "type1", null, """{"stag": "111", "bstag": 111, "shtag": 111, "lstag": 111, "fstag": 111.1, "dstag": 111.1, "tag": ["xxx", "yyy"], "ltag": [3000, 2000], "dtag": [3000.1, 2000.1]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      var response = indexer.search(query=termQuery("stag", "111"), facets=Seq(termsFacet("facet1").field("stag").size(10).executionHint(executionHint), termsFacet("facet2").field("tag").size(10).executionHint(executionHint)))
      var facet: TermsFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.getTotalCount should be === (2)
      facet.getOtherCount should be === (0)
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      facet = response.facets().facet("facet2")
      facet.name() should be === ("facet2")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (2)
      response = indexer.search(query=termQuery("stag", "111"), facets=Seq(
        termsFacet("facet1").field("lstag").size(10).executionHint(executionHint),
        termsFacet("facet2").field("ltag").size(10).executionHint(executionHint),
        termsFacet("facet3").field("ltag").size(10).exclude(3000.asInstanceOf[Object]).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.getClass should be === classOf[InternalLongTermsFacet]
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      facet = response.facets().facet("facet2")
      facet.getClass should be === classOf[InternalLongTermsFacet]
      facet.name() should be === ("facet2")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("2000")
      facet.entries().get(0).count() should be === (2)
      Set("1000", "3000") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
      Set("1000", "3000") should contain (facet.entries().get(2).term)
      facet.entries().get(2).count() should be === (1)
      facet = response.facets().facet("facet3")
      facet.getClass should be === classOf[InternalLongTermsFacet]
      facet.name() should be === ("facet3")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("2000")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).term() should be === ("1000")
      facet.entries().get(1).count() should be === (1)
      response = indexer.search(query=termQuery("stag", "111"), facets=Seq(termsFacet("facet1").field("dstag").size(10).executionHint(executionHint), termsFacet("facet2").field("dtag").size(10).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.getClass should be === classOf[InternalDoubleTermsFacet]
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111.1")
      facet.entries().get(0).count() should be === (2)
      facet = response.facets().facet("facet2")
      facet.getClass should be === classOf[InternalDoubleTermsFacet]
      facet.name() should be === ("facet2")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("2000.1")
      facet.entries().get(0).count() should be === (2)
      Set("1000.1", "3000.1") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
      Set("1000.1", "3000.1") should contain (facet.entries().get(2).term)
      facet.entries().get(2).count() should be === (1)
      response = indexer.search(query = termQuery("stag", "111"), facets = Seq(termsFacet("facet1").field("bstag").size(10).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.getClass should be === classOf[InternalByteTermsFacet]
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      response = indexer.search(query = termQuery("stag", "111"), facets = Seq(termsFacet("facet1").field("istag").size(10).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.getClass should be === classOf[InternalIntTermsFacet]
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      response = indexer.search(query = termQuery("stag", "111"), facets = Seq(termsFacet("facet1").field("shstag").size(10).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.getClass should be === classOf[InternalShortTermsFacet]
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("stag").size(10).facetFilter(termFilter("tag","xxx")).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("stag").size(10).facetFilter(termFilter("tag","xxx")).global(true).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("type1.stag").size(10).facetFilter(termFilter("tag","xxx")).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (1)
      response = indexer.search(facets = Seq(termsFacet("facet1").field("tag").size(10).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (2)
      Set("xxx", "yyy") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
      Set("xxx", "yyy") should contain (facet.entries().get(2).term)
      facet.entries().get(2).count() should be === (1)
      response = indexer.search(facets = Seq(termsFacet("facet1").field("tag").size(2).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (2)
      Set("xxx", "yyy") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("tag").size(10).exclude("yyy") .executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      Set("xxx", "zzz") should contain (facet.entries().get(0).term)
      facet.entries().get(0).count() should be === (1)
      Set("xxx", "zzz") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("tag").size(10).order(TermsFacet.ComparatorType.TERM) .executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(2).term() should be === ("zzz")
      facet.entries().get(2).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("tag").size(10).order(TermsFacet.ComparatorType.REVERSE_TERM) .executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      facet.entries().get(2).term() should be === ("xxx")
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(0).term() should be === ("zzz")
      facet.entries().get(0).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("tag").size(10).script("term + param1").param("param1", "a").order(TermsFacet.ComparatorType.TERM).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("xxxa")
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(1).term() should be === ("yyya")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(2).term() should be === ("zzza")
      facet.entries().get(2).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").field("tag").size(10).script("term == 'xxx' ? false : true").order(TermsFacet.ComparatorType.TERM).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).term() should be === ("zzz")
      facet.entries().get(1).count() should be === (1)
      response = indexer.search(facets=Seq(termsFacet("facet1").fields("stag", "tag").size(10).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (4)
      Set("111", "yyy") should contain (facet.entries().get(0).term)
      facet.entries().get(0).count() should be === (2)
      Set("111", "yyy") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (2)
      Set("zzz", "xxx") should contain (facet.entries().get(2).term)
      facet.entries().get(2).count() should be === (1)
      Set("zzz", "xxx") should contain (facet.entries().get(3).term)
      facet.entries().get(3).count() should be === (1)
      response = indexer.search(query=termQuery("xxx", "yyy"), facets=Seq(termsFacet("facet1").field("tag").size(10).allTerms(true).executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      Set("xxx", "yyy", "zzz") should contain (facet.entries().get(0).term)
      facet.entries().get(0).count() should be === (0)
      Set("xxx", "yyy", "zzz") should contain (facet.entries().get(1).term)
      facet.entries().get(1).count() should be === (0)
      Set("xxx", "yyy", "zzz") should contain (facet.entries().get(2).term)
      facet.entries().get(2).count() should be === (0)
      response = indexer.search(facets=Seq(termsFacet("facet1").scriptField("_source.stag").size(10).executionHint(executionHint), termsFacet("facet2").scriptField("_source.tag").size(10)
          .executionHint(executionHint)))
      facet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (1)
      facet.entries().get(0).term() should be === ("111")
      facet.entries().get(0).count() should be === (2)
      facet = response.facets().facet("facet2")
      facet.name() should be === ("facet2")
      facet.entries().size should be === (3)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (2)
    }
  }

  test("testTermFacetWithEqualTermDistribution") {
    for (i <- 0 until 5) indexer.index(indexName, "type1", null, """{"text": "foo bar"}""")
    for (i <- 0 until 5) indexer.index(indexName, "type1", null, """{"text": "bar baz"}""")
    for (i <- 0 until 5) indexer.index(indexName, "type1", null, """{"text": "baz foo"}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets = Seq(termsFacet("facet1").field("text").size(10)))
      var facet: TermsFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      for (j <- 0 until 3) {
    	  Set("foo", "bar", "baz") should contain (facet.entries().get(j).term)
        facet.entries().get(j).count() should be === (10)
      }
    }
  }

  test("testStatsFacets") {
    pending //fixme: failing test
    indexer.index(indexName, "type1", null, """{"num": 1, "multi_num": [1.0, 2.0]}""")
    indexer.index(indexName, "type1", null, """{"num": 1, "multi_num": [3.0, 4.0]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      var response = indexer.search(facets=Seq(
          statisticalFacet("stats1").field("num"), 
          statisticalFacet("stats2").field("multi_num"), 
          statisticalScriptFacet("stats3").script("doc['num'].value * 2")))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: StatisticalFacet = response.facets().facet("stats1")
      facet.name() should be === (facet.name())
      facet.count() should be === (2)
      facet.total() should be === (3.0)
      facet.min() should be === (1.0)
      facet.max() should be === (2.0)
      facet.mean() should be === (1.5d)
      facet.sumOfSquares() should be === (5.0)
      facet = response.facets().facet("stats2")
      facet.name() should be === (facet.name())
      facet.count() should be === (4)
      facet.total() should be === (10.0)
      facet.min() should be === (1.0)
      facet.max() should be === (4.0)
      facet.mean() should be === (2.5d)
      facet = response.facets().facet("stats3")
      facet.name() should be === (facet.name())
      facet.count() should be === (2)
      facet.total() should be === (6.0)
      facet.min() should be === (2.0)
      facet.max() should be === (4.0)
      facet.mean() should be === (3.0)
      facet.sumOfSquares() should be === (20.0)
      response = indexer.search(facets=Seq(statisticalFacet("stats").fields("num", "multi_num")))
      facet = response.facets().facet("stats")
      facet.name() should be === (facet.name())
      facet.count() should be === (6)
      facet.total() should be === (13.0)
      facet.min() should be === (1.0)
      facet.max() should be === (4.0)
      facet.mean() should be === (13d / 6d)
      facet.sumOfSquares() should be === (35.0)
      response = indexer.search(facets = Seq(statisticalFacet("stats").field("num"), statisticalFacet("stats").field("multi_num")))
      facet = response.facets().facet("stats")
      facet.name() should be === (facet.name())
      facet.count() should be === (6)
      facet.total() should be === (13.0)
      facet.min() should be === (1.0)
      facet.max() should be === (4.0)
      facet.mean() should be === (13d / 6d)
      facet.sumOfSquares() should be === (35.0)
    }
  }

  test("testHistoFacetEdge") {
    indexer.index(indexName, "type1", null, """{"num": 100}""")
    indexer.index(indexName, "type1", null, """{"num": 200}""")
    indexer.index(indexName, "type1", null, """{"num": 300}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(histogramFacet("facet1").field("num").valueField("num").interval(100)))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: HistogramFacet = response.facets().facet("facet1")
      facet.name() should be === ("facet1")
      facet.entries().size should be === (3)
      facet.entries().get(0).key() should be === (100)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(1).key() should be === (200)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(2).key() should be === (300)
      facet.entries().get(2).count() should be === (1)
    }
  }

  test("testHistoFacets") {
    pending //fixme: failing test
    indexer.index(indexName, "type1", null, """{"num": 1055, "date": "1970-01-01T00:00:00", "multi_num": [13.0, 23.0]}""")
    indexer.index(indexName, "type1", null, """{"num": 1065, "date": "1970-01-01T00:00:25", "multi_num": [15.0, 31.0]}""")
    indexer.index(indexName, "type1", null, """{"num": 1075, "date": "1970-01-01T00:02:00", "multi_num": [17.0, 25.0]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(
        histogramFacet("stats1").field("num").valueField("num").interval(100),
        histogramFacet("stats2").field("multi_num").valueField("multi_num").interval(10),
        histogramFacet("stats3").keyField("num").valueField("multi_num").interval(100),
        histogramScriptFacet("stats4").keyScript("doc['date'].date.minuteOfHour").valueScript("doc['num'].value"),
        histogramFacet("stats5").field("date").interval(1, TimeUnit.MINUTES),
        histogramScriptFacet("stats6").keyField("num").valueScript("doc['num'].value").interval(100),
        histogramFacet("stats7").field("num").interval(100),
        histogramScriptFacet("stats8").keyField("num").valueScript("doc.score").interval(100),
        histogramFacet("stats9").field("num").bounds(1000, 1200).interval(100),
        histogramFacet("stats10").field("num").bounds(1000, 1300).interval(100),
        histogramFacet("stats11").field("num").valueField("num").bounds(1000, 1300).interval(100),
        histogramScriptFacet("stats12").keyField("num").valueScript("doc['num'].value").bounds(1000, 1300).interval(100),
        histogramFacet("stats13").field("num").bounds(1056, 1176).interval(100),
        histogramFacet("stats14").field("num").valueField("num").bounds(1056, 1176).interval(100)))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: HistogramFacet = null
      facet = response.facets().facet("stats1")
      facet.name() should be === ("stats1")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).min() should be (1055d plusOrMinus 0.000001)
      facet.entries().get(0).max() should be (1065d plusOrMinus 0.000001)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be === (2120.0)
      facet.entries().get(0).mean() should be === (1060.0)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).min() should be (1175d plusOrMinus 0.000001)
      facet.entries().get(1).max() should be (1175d plusOrMinus 0.000001)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).total() should be === (1175.0)
      facet.entries().get(1).mean() should be === (1175.0)
      facet = response.facets().facet("stats2")
      facet.name() should be === ("stats2")
      facet.entries().size should be === (3)
      facet.entries().get(0).key() should be === (10)
      facet.entries().get(0).count() should be === (3)
      facet.entries().get(0).totalCount() should be === (3)
      facet.entries().get(0).total() should be === (45.0)
      facet.entries().get(0).mean() should be === (15.0)
      facet.entries().get(1).key() should be === (20)
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).totalCount() should be === (2)
      facet.entries().get(1).total() should be === (48.0)
      facet.entries().get(1).mean() should be === (24.0)
      facet.entries().get(2).key() should be === (30)
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(2).totalCount() should be === (1)
      facet.entries().get(2).total() should be === (31.0)
      facet.entries().get(2).mean() should be === (31.0)
      facet = response.facets().facet("stats3")
      facet.name() should be === ("stats3")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).totalCount() should be === (4)
      facet.entries().get(0).total() should be === (82.0)
      facet.entries().get(0).mean() should be === (20.5d)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).totalCount() should be === (2)
      facet.entries().get(1).total() should be === (42.0)
      facet.entries().get(1).mean() should be === (21.0)
      facet = response.facets().facet("stats4")
      facet.name() should be === ("stats4")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (0)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be === (2120.0)
      facet.entries().get(0).mean() should be === (1060.0)
      facet.entries().get(1).key() should be === (2)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).total() should be === (1175.0)
      facet.entries().get(1).mean() should be === (1175.0)
      facet = response.facets().facet("stats5")
      facet.name() should be === ("stats5")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (0)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).key() should be === (TimeValue.timeValueMinutes(2).millis())
      facet.entries().get(1).count() should be === (1)
      facet = response.facets().facet("stats6")
      facet.name() should be === ("stats6")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be === (2120.0)
      facet.entries().get(0).mean() should be === (1060.0)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).total() should be === (1175.0)
      facet.entries().get(1).mean() should be === (1175.0)
      facet = response.facets().facet("stats7")
      facet.name() should be === ("stats7")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet = response.facets().facet("stats8")
      facet.name() should be === ("stats8")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be === (2.0)
      facet.entries().get(0).mean() should be === (1.0)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).total() should be === (1.0)
      facet.entries().get(1).mean() should be === (1.0)
      facet = response.facets().facet("stats9")
      facet.name() should be === ("stats9")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet = response.facets().facet("stats10")
      facet.name() should be === ("stats10")
      facet.entries().size should be === (3)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(2).key() should be === (1200)
      facet.entries().get(2).count() should be === (0)
      facet = response.facets().facet("stats11")
      facet.name() should be === ("stats11")
      facet.entries().size should be === (3)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).min() should be (1055d plusOrMinus 0.000001)
      facet.entries().get(0).max() should be (1065d plusOrMinus 0.000001)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be === (2120.0)
      facet.entries().get(0).mean() should be === (1060.0)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).min() should be (1175d plusOrMinus 0.000001)
      facet.entries().get(1).max() should be (1175d plusOrMinus 0.000001)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).total() should be === (1175.0)
      facet.entries().get(1).mean() should be === (1175.0)
      facet.entries().get(2).key() should be === (1200)
      facet.entries().get(2).count() should be === (0)
      facet.entries().get(2).totalCount() should be === (0)
      facet = response.facets().facet("stats12")
      facet.name() should be === ("stats12")
      facet.entries().size should be === (3)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).min() should be (1055d plusOrMinus 0.000001)
      facet.entries().get(0).max() should be (1065d plusOrMinus 0.000001)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be === (2120.0)
      facet.entries().get(0).mean() should be === (1060.0)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).min() should be (1175d plusOrMinus 0.000001)
      facet.entries().get(1).max() should be (1175d plusOrMinus 0.000001)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).total() should be === (1175.0)
      facet.entries().get(1).mean() should be === (1175.0)
      facet.entries().get(2).key() should be === (1200)
      facet.entries().get(2).count() should be === (0)
      facet.entries().get(2).totalCount() should be === (0)
      facet = response.facets().facet("stats13")
      facet.name() should be === ("stats13")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
      facet = response.facets().facet("stats14")
      facet.name() should be === ("stats14")
      facet.entries().size should be === (2)
      facet.entries().get(0).key() should be === (1000)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(1).key() should be === (1100)
      facet.entries().get(1).count() should be === (1)
    }
  }

  test("testRangeFacets") {
    pending //fixme: failing test
    indexer.index(indexName, "type1", null, """{"num": 1055, "date": "1970-01-01T00:00:00", "multi_num": [13.0, 23.0], "multi_value": [10, 11]}""")
    indexer.index(indexName, "type1", null, """{"num": 1065, "date": "1970-01-01T00:00:25", "multi_num": [15.0, 31.0], "multi_value": [20, 21]}""")
    indexer.index(indexName, "type1", null, """{"num": 1075, "date": "1970-01-01T00:02:00", "multi_num": [17.0, 25.0], "multi_value": [30, 31]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(
    	rangeFacet("range1").field("num").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170),
        rangeFacet("range2").keyField("num").valueField("value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170),
        rangeFacet("range3").keyField("num").valueField("multi_value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170),
        rangeFacet("range4").keyField("multi_num").valueField("value").addUnboundedFrom(16).addRange(10, 26).addUnboundedTo(20),
        rangeScriptFacet("range5").keyScript("doc['num'].value").valueScript("doc['value'].value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170),
        rangeFacet("range6").field("date").addUnboundedFrom("1970-01-01T00:00:26").addRange("1970-01-01T00:00:15", "1970-01-01T00:00:53").addUnboundedTo("1970-01-01T00:00:26")))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: RangeFacet = response.facets().facet("range1")
      facet.name() should be === ("range1")
      facet.entries().size should be === (3)
      facet.entries().get(0).to() should be (1056.0 plusOrMinus 0.000001)
      facet.entries().get(0).toAsString().toDouble should be (1056.0 plusOrMinus 0.000001)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).totalCount() should be === (1)
      facet.entries().get(0).total() should be (1055.0 plusOrMinus 0.000001)
      facet.entries().get(0).min() should be (1055.0 plusOrMinus 0.000001)
      facet.entries().get(0).max() should be (1055.0 plusOrMinus 0.000001)
      facet.entries().get(1).from() should be (1000.0 plusOrMinus 0.000001)
      facet.entries().get(1).fromAsString().toDouble should be (1000.0 plusOrMinus 0.000001)
      facet.entries().get(1).to() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(1).toAsString().toDouble should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).totalCount() should be === (2)
      facet.entries().get(1).total() should be ((1055.0 + 1065.0) plusOrMinus 0.000001)
      facet.entries().get(1).min() should be (1055.0 plusOrMinus 0.000001)
      facet.entries().get(1).max() should be (1065.0 plusOrMinus 0.000001)
      facet.entries().get(2).from() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(2).totalCount() should be === (1)
      facet.entries().get(2).total() should be (1175.0 plusOrMinus 0.000001)
      facet.entries().get(2).min() should be (1175.0 plusOrMinus 0.000001)
      facet.entries().get(2).max() should be (1175.0 plusOrMinus 0.000001)
      facet = response.facets().facet("range2")
      facet.name() should be === ("range2")
      facet.entries().size should be === (3)
      facet.entries().get(0).to() should be (1056.0 plusOrMinus 0.000001)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).total() should be (1.0 plusOrMinus 0.000001)
      facet.entries().get(1).from() should be (1000.0 plusOrMinus 0.000001)
      facet.entries().get(1).to() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).total() should be (3.0 plusOrMinus 0.000001)
      facet.entries().get(2).from() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(2).total() should be (3.0 plusOrMinus 0.000001)
      facet = response.facets().facet("range3")
      facet.name() should be === ("range3")
      facet.entries().size should be === (3)
      facet.entries().get(0).to() should be (1056.0 plusOrMinus 0.000001)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).total() should be ((10.0 + 11.0) plusOrMinus 0.000001)
      facet.entries().get(0).min() should be (10.0 plusOrMinus 0.000001)
      facet.entries().get(0).max() should be (11.0 plusOrMinus 0.000001)
      facet.entries().get(1).from() should be (1000.0 plusOrMinus 0.000001)
      facet.entries().get(1).to() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).totalCount() should be === (4)
      facet.entries().get(1).total() should be (62.0 plusOrMinus 0.000001)
      facet.entries().get(1).min() should be (10.0 plusOrMinus 0.000001)
      facet.entries().get(1).max() should be (21.0 plusOrMinus 0.000001)
      facet.entries().get(2).from() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(2).totalCount() should be === (2)
      facet.entries().get(2).total() should be (61.0 plusOrMinus 0.000001)
      facet.entries().get(2).min() should be (30.0 plusOrMinus 0.000001)
      facet.entries().get(2).max() should be (31.0 plusOrMinus 0.000001)
      facet = response.facets().facet("range4")
      facet.name() should be === ("range4")
      facet.entries().size should be === (3)
      facet.entries().get(0).to() should be (16.0 plusOrMinus 0.000001)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (3.0 plusOrMinus 0.000001)
      facet.entries().get(1).from() should be (10.0 plusOrMinus 0.000001)
      facet.entries().get(1).to() should be (26.0 plusOrMinus 0.000001)
      facet.entries().get(1).count() should be === (3)
      facet.entries().get(1).total() should be ((1.0 + 2.0 + 3.0) plusOrMinus 0.000001)
      facet.entries().get(2).from() should be (20.0 plusOrMinus 0.000001)
      facet.entries().get(2).count() should be === (3)
      facet.entries().get(2).total() should be ((1.0 + 2.0 + 3.0) plusOrMinus 0.000001)
      facet = response.facets().facet("range5")
      facet.name() should be === ("range5")
      facet.entries().size should be === (3)
      facet.entries().get(0).to() should be (1056.0 plusOrMinus 0.000001)
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).total() should be (1.0 plusOrMinus 0.000001)
      facet.entries().get(1).from() should be (1000.0 plusOrMinus 0.000001)
      facet.entries().get(1).to() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).total() should be (3.0 plusOrMinus 0.000001)
      facet.entries().get(2).from() should be (1170.0 plusOrMinus 0.000001)
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(2).total() should be (3.0 plusOrMinus 0.000001)
      facet = response.facets().facet("range6")
      facet.name() should be === ("range6")
      facet.entries().size should be === (3)
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).toAsString() should be === ("1970-01-01T00:00:26")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).fromAsString() should be === ("1970-01-01T00:00:15")
      facet.entries().get(1).toAsString() should be === ("1970-01-01T00:00:53")
      facet.entries().get(2).count() should be === (1)
      facet.entries().get(2).fromAsString() should be === ("1970-01-01T00:00:26")
    }
  }

  test("testDateHistoFacets") {
    indexer.index(indexName, "type1", null, """{"date": "2009-03-05T01:01:01", "num": 1}""")
    indexer.index(indexName, "type1", null, """{"date": "2009-03-05T04:01:01", "num": 2}""")
    indexer.index(indexName, "type1", null, """{"date": "2009-03-06T01:01:01", "num": 3}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(
        dateHistogramFacet("stats1").field("date").interval("day"),
        dateHistogramFacet("stats2").field("date").interval("day").preZone("-02:00"),
        dateHistogramFacet("stats3").field("date").valueField("num").interval("day").preZone("-02:00"),
        dateHistogramFacet("stats4").field("date").valueScript("doc['num'].value * 2").interval("day").preZone("-02:00"),
        dateHistogramFacet("stats5").field("date").interval("24h"),
        dateHistogramFacet("stats6").field("date").valueField("num").interval("day").preZone("-02:00").postZone("-02:00"),
        dateHistogramFacet("stats7").field("date").interval("quarter")))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      //      var facet: DateHistogramFacet = response.facets().facet("stats1")
      //      facet.name() should be === ("stats1")
      //      facet.entries().size should be === (2)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-03-05"))
      //      facet.entries().get(0).count() should be === (2)
      //      facet.entries().get(1).time() should be === (utcTimeInMillis("2009-03-06"))
      //      facet.entries().get(1).count() should be === (1)
      //      facet = response.facets().facet("stats2")
      //      facet.name() should be === ("stats2")
      //      facet.entries().size should be === (2)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-03-04"))
      //      facet.entries().get(0).count() should be === (1)
      //      facet.entries().get(1).time() should be === (utcTimeInMillis("2009-03-05"))
      //      facet.entries().get(1).count() should be === (2)
      //      facet = response.facets().facet("stats3")
      //      facet.name() should be === ("stats3")
      //      facet.entries().size should be === (2)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-03-04"))
      //      facet.entries().get(0).count() should be === (1)
      //      facet.entries().get(0).total() should be === (1.0)
      //      facet.entries().get(1).time() should be === (utcTimeInMillis("2009-03-05"))
      //      facet.entries().get(1).count() should be === (2)
      //      facet.entries().get(1).total() should be === (5.0)
      //      facet = response.facets().facet("stats4")
      //      facet.name() should be === ("stats4")
      //      facet.entries().size should be === (2)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-03-04"))
      //      facet.entries().get(0).count() should be === (1)
      //      facet.entries().get(0).total() should be === (2.0)
      //      facet.entries().get(1).time() should be === (utcTimeInMillis("2009-03-05"))
      //      facet.entries().get(1).count() should be === (2)
      //      facet.entries().get(1).total() should be === (10.0)
      //      facet = response.facets().facet("stats5")
      //      facet.name() should be === ("stats5")
      //      facet.entries().size should be === (2)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-03-05"))
      //      facet.entries().get(0).count() should be === (2)
      //      facet.entries().get(1).time() should be === (utcTimeInMillis("2009-03-06"))
      //      facet.entries().get(1).count() should be === (1)
      //      facet = response.facets().facet("stats6")
      //      facet.name() should be === ("stats6")
      //      facet.entries().size should be === (2)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-03-04") - TimeValue.timeValueHours(2).millis())
      //      facet.entries().get(0).count() should be === (1)
      //      facet.entries().get(0).total() should be === (1.0)
      //      facet.entries().get(1).time() should be === (utcTimeInMillis("2009-03-05") - TimeValue.timeValueHours(2).millis())
      //      facet.entries().get(1).count() should be === (2)
      //      facet.entries().get(1).total() should be === (5.0)
      //      facet = response.facets().facet("stats7")
      //      facet.name() should be === ("stats7")
      //      facet.entries().size should be === (1)
      //      facet.entries().get(0).time() should be === (utcTimeInMillis("2009-01-01"))
    }
  }

  test("testTermsStatsFacets") {
    indexer.index(indexName, "type1", null, """{"field": "xxx", "num": 100.0, "multi_num": [1.0, 2.0]}""")
    indexer.index(indexName, "type1", null, """{"field": "xxx", "num": 200.0, "multi_num": [2.0, 3.0]}""")
    indexer.index(indexName, "type1", null, """{"field": "yyy", "num": 500.0, "multi_num": [5.0, 6.0]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(
        termsStatsFacet("stats1").keyField("field").valueField("num"),
        termsStatsFacet("stats2").keyField("field").valueField("multi_num"),
        termsStatsFacet("stats3").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.COUNT),
        termsStatsFacet("stats4").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.COUNT),
        termsStatsFacet("stats5").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.TOTAL),
        termsStatsFacet("stats6").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.TOTAL),
        termsStatsFacet("stats7").keyField("field").valueField("num").allTerms(),
        termsStatsFacet("stats8").keyField("field").valueField("multi_num").allTerms(),
        termsStatsFacet("stats9").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.COUNT).allTerms(),
        termsStatsFacet("stats10").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.COUNT).allTerms(),
        termsStatsFacet("stats11").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.TOTAL).allTerms(),
        termsStatsFacet("stats12").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.TOTAL).allTerms(),
        termsStatsFacet("stats13").keyField("field").valueScript("doc['num'].value * 2")))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: TermsStatsFacet = response.facets().facet("stats1")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).totalCount() should be === (2)
      facet.entries().get(0).min() should be (100d plusOrMinus 0.00001d)
      facet.entries().get(0).max() should be (200d plusOrMinus 0.00001d)
      facet.entries().get(0).total() should be (300d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).totalCount() should be === (1)
      facet.entries().get(1).min() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).max() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).total() should be (500d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats2")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).min() should be (1d plusOrMinus 0.00001d)
      facet.entries().get(0).max() should be (3d plusOrMinus 0.00001d)
      facet.entries().get(0).total() should be (8d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).min() should be (5d plusOrMinus 0.00001d)
      facet.entries().get(1).max() should be (6d plusOrMinus 0.00001d)
      facet.entries().get(1).total() should be (11d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats3")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (300d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (500d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats4")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (8d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (11d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats5")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).total() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("xxx")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).total() should be (300d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats6")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).total() should be (11d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("xxx")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).total() should be (8d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats7")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (300d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (500d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats8")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (8d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (11d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats9")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (300d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (500d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats10")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (8d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (11d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats11")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).total() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("xxx")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).total() should be (300d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats12")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("yyy")
      facet.entries().get(0).count() should be === (1)
      facet.entries().get(0).total() should be (11d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("xxx")
      facet.entries().get(1).count() should be === (2)
      facet.entries().get(1).total() should be (8d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats13")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("xxx")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).total() should be (600d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("yyy")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).total() should be (1000d plusOrMinus 0.00001d)
    }
  }

  test("testNumericTermsStatsFacets") {
    pending //fixme: failing test
    indexer.index(indexName, "type1", null, """{"lField": 100, "dField": 100.1, "num": 100.0, "multi_num": [1.0, 2.0]}""")
    indexer.index(indexName, "type1", null, """{"lField": 100, "dField": 100.1, "num": 200.0, "multi_num": [2.0, 3.0]}""")
    indexer.index(indexName, "type1", null, """{"lField": 100, "dField": 100.1, "num": 500.0, "multi_num": [5.0, 6.0]}""")
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets = Seq(termsStatsFacet("stats1").keyField("lField").valueField("num"), termsStatsFacet("stats2").keyField("dField").valueField("num")))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: TermsStatsFacet = response.facets().facet("stats1")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("100")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).min() should be (100d plusOrMinus 0.00001d)
      facet.entries().get(0).max() should be (200d plusOrMinus 0.00001d)
      facet.entries().get(0).total() should be (300d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("200")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).min() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).max() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).total() should be (500d plusOrMinus 0.00001d)
      facet = response.facets().facet("stats2")
      facet.entries().size should be === (2)
      facet.entries().get(0).term() should be === ("100.1")
      facet.entries().get(0).count() should be === (2)
      facet.entries().get(0).min() should be (100d plusOrMinus 0.00001d)
      facet.entries().get(0).max() should be (200d plusOrMinus 0.00001d)
      facet.entries().get(0).total() should be (300d plusOrMinus 0.00001d)
      facet.entries().get(1).term() should be === ("200.2")
      facet.entries().get(1).count() should be === (1)
      facet.entries().get(1).min() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).max() should be (500d plusOrMinus 0.00001d)
      facet.entries().get(1).total() should be (500d plusOrMinus 0.00001d)
    }
  }

  test("testTermsStatsFacets2") {
    for (i <- 0 until 20) indexer.index(indexName, "type1", i.toString, """{"num": %s}""".format(i % 10))
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      val response = indexer.search(facets=Seq(
	      termsStatsFacet("stats1").keyField("num").valueScript("doc.score").order(TermsStatsFacet.ComparatorType.COUNT), 
	      termsStatsFacet("stats2").keyField("num").valueScript("doc.score").order(TermsStatsFacet.ComparatorType.TOTAL)))
      if (response.failedShards() > 0) {
        //logger.warn("Failed shards:")
        for (shardSearchFailure <- response.shardFailures) {
          //logger.warn("-> {}", shardSearchFailure)
        }
      }
      response.failedShards() should be === (0)
      var facet: org.elasticsearch.search.facet.termsstats.longs.InternalTermsStatsLongFacet = response.facets().facet("stats1")
      facet.entries().size should be === (10)
      facet = response.facets().facet("stats2")
      facet.entries().size should be === (10)
    }
  }

  test("testQueryFacet") {
    pending //fixme: failing test
    for (i <- 0 until 20) indexer.index(indexName, "type1", i.toString, """{"num": %s}""".format(i % 10))
    indexer.refresh()
    for (i <- 0 until numberOfRuns()) {
      var response = indexer.search(facets=Seq(queryFacet("query").query(termQuery("num", 1))))
      var facet: QueryFacet = response.facets().facet("query")
      facet.count() should be === (2)
      response = indexer.search(facets = Seq(queryFacet("query").query(termQuery("num", 1)).global(true)))
      facet = response.facets().facet("query")
      facet.count() should be === (2)
      response = indexer.search(facets=Seq(queryFacet("query").query(termsQuery("num", Array(1, 2))).facetFilter(termFilter("num", 1)).global(true)))
      facet = response.facets().facet("query")
      facet.count() should be === (2)
    }
  }

}
