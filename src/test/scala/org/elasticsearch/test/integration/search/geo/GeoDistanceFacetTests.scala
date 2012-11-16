package org.elasticsearch.test.integration.search.geo

import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.facet.FacetBuilders._
import org.elasticsearch.search.facet.geodistance._
import org.elasticsearch.action.search._
import org.elasticsearch.common.unit._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class GeoDistanceFacetTests extends IndexerBasedTest {
  val mapping = """{"type1": {"properties": {"location": {"type": "geo_point", "lat_lon": true}}}}"""

  test("simpleGeoFacetTests") {
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"name": "New York", "num": 1, "location": {"lat": 40.7143528, "lon": -74.0059731}}""")
    indexer.index(indexName, "type1", "2", """{"name": "Times Square", "num": 2, "location": {"lat": 40.759011, "lon": -73.9844722}}""")
    indexer.index(indexName, "type1", "3", """{"name": "Tribeca", "num": 3, "location": {"lat": 40.718266, "lon": -74.007819}}""")
    indexer.index(indexName, "type1", "4", """{"name": "Wall Street", "num": 4, "location": {"lat": 40.7051157, "lon": -74.0088305}}""")
    indexer.index(indexName, "type1", "5", """{"name": "Soho", "num": 5, "location": {"lat": 40.7247222, "lon": -74}}""")
    indexer.index(indexName, "type1", "6", """{"name": "Greenwich Village", "num": 6, "location": {"lat": 40.731033, "lon": -73.9962255}}""")
    indexer.index(indexName, "type1", "7", """{"name": "Brooklyn", "num": 7, "location": {"lat": 40.65, "lon": -73.95}}""")
    indexer.refresh()
    var response = indexer.search_prepare().setQuery(matchAllQuery).addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528,
      -74.0059731)
      .unit(DistanceUnit.KILOMETERS)
      .addUnboundedFrom(2)
      .addRange(0, 1)
      .addRange(0.5, 2.5)
      .addUnboundedTo(1)).execute.actionGet
    response.hits.totalHits should be === (7)
    var facet: GeoDistanceFacet = response.facets().facet("geo1")
    facet.entries().size should be === (4)
    facet.entries().get(0).to() should be(2.0 plusOrMinus 0.000001)
    facet.entries().get(0).count() should be === (4)
    facet.entries().get(0).total() should not be (0.0 plusOrMinus 0.00001)
    facet.entries().get(1).from() should be(0.0 plusOrMinus 0.000001)
    facet.entries().get(1).to() should be(1.0 plusOrMinus 0.000001)
    facet.entries().get(1).count() should be === (2)
    facet.entries().get(1).total() should not be (0.0 plusOrMinus 0.00001)
    facet.entries().get(2).from() should be(0.5 plusOrMinus 0.000001)
    facet.entries().get(2).to() should be(2.5 plusOrMinus 0.000001)
    facet.entries().get(2).count() should be === (3)
    facet.entries().get(2).total() should not be (0.0 plusOrMinus 0.00001)
    facet.entries().get(3).from() should be(1.0 plusOrMinus 0.000001)
    facet.entries().get(3).count() should be === (5)
    facet.entries().get(3).total() should not be (0.0 plusOrMinus 0.00001)
    response = indexer.search_prepare().setQuery(matchAllQuery).addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528,
      -74.0059731)
      .unit(DistanceUnit.KILOMETERS)
      .valueField("num")
      .addUnboundedFrom(2)
      .addRange(0, 1)
      .addRange(0.5, 2.5)
      .addUnboundedTo(1)).execute.actionGet
    response.hits.totalHits should be === (7)
    facet = response.facets().facet("geo1")
    facet.entries().size should be === (4)
    facet.entries().get(0).to() should be(2.0 plusOrMinus 0.000001)
    facet.entries().get(0).count() should be === (4)
    facet.entries().get(0).total() should be(13.0 plusOrMinus 0.00001)
    facet.entries().get(1).from() should be(0.0 plusOrMinus 0.000001)
    facet.entries().get(1).to() should be(1.0 plusOrMinus 0.000001)
    facet.entries().get(1).count() should be === (2)
    facet.entries().get(1).total() should be(4.0 plusOrMinus 0.00001)
    facet.entries().get(2).from() should be(0.5 plusOrMinus 0.000001)
    facet.entries().get(2).to() should be(2.5 plusOrMinus 0.000001)
    facet.entries().get(2).count() should be === (3)
    facet.entries().get(2).total() should be(15.0 plusOrMinus 0.00001)
    facet.entries().get(3).from() should be(1.0 plusOrMinus 0.000001)
    facet.entries().get(3).count() should be === (5)
    facet.entries().get(3).total() should be(24.0 plusOrMinus 0.00001)
    response = indexer.search_prepare().setQuery(matchAllQuery).addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528,
      -74.0059731)
      .unit(DistanceUnit.KILOMETERS)
      .valueScript("doc['num'].value")
      .addUnboundedFrom(2)
      .addRange(0, 1)
      .addRange(0.5, 2.5)
      .addUnboundedTo(1)).execute.actionGet
    response.hits.totalHits should be === (7)
    facet = response.facets().facet("geo1")
    facet.entries().size should be === (4)
    facet.entries().get(0).to() should be(2.0 plusOrMinus 0.000001)
    facet.entries().get(0).count() should be === (4)
    facet.entries().get(0).total() should be(13.0 plusOrMinus 0.00001)
    facet.entries().get(1).from() should be(0.0 plusOrMinus 0.000001)
    facet.entries().get(1).to() should be(1.0 plusOrMinus 0.000001)
    facet.entries().get(1).count() should be === (2)
    facet.entries().get(1).total() should be(4.0 plusOrMinus 0.00001)
    facet.entries().get(2).from() should be(0.5 plusOrMinus 0.000001)
    facet.entries().get(2).to() should be(2.5 plusOrMinus 0.000001)
    facet.entries().get(2).count() should be === (3)
    facet.entries().get(2).total() should be(15.0 plusOrMinus 0.00001)
    facet.entries().get(3).from() should be(1.0 plusOrMinus 0.000001)
    facet.entries().get(3).count() should be === (5)
    facet.entries().get(3).total() should be(24.0 plusOrMinus 0.00001)
  }

  test("multiLocationGeoDistanceTest") {
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"num": 1, "location": [{"lat": 40.7143528, "lon": -74.0059731}, {"lat": 40.759011, "lon": -73.9844722}]}""")
    indexer.index(indexName, "type1", "3", """{"num": 3, "location": [{"lat": 40.718266, "lon": -74.007819}, {"lat": 40.7051157, "lon": -74.0088305}]}""")
    indexer.refresh()
    val response = indexer.search_prepare().setQuery(matchAllQuery).addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528,
      -74.0059731)
      .unit(DistanceUnit.KILOMETERS)
      .addRange(0, 2)
      .addRange(2, 10)).execute.actionGet

    response.failedShards() should be === (0)
    response.hits.totalHits should be === (2)
    val facet: GeoDistanceFacet = response.facets().facet("geo1")
    facet.entries().size should be === (2)
    facet.entries().get(0).from() should be(0.0 plusOrMinus 0.000001)
    facet.entries().get(0).to() should be(2.0 plusOrMinus 0.000001)
    facet.entries().get(0).count() should be === (2)
    facet.entries().get(1).from() should be(2.0 plusOrMinus 0.000001)
    facet.entries().get(1).to() should be(10.0 plusOrMinus 0.000001)
    facet.entries().get(1).count() should be === (1)
  }
}
