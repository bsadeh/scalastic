package org.elasticsearch.test.integration.search.geo

import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.index.mapper.geo._
import org.elasticsearch.index.search.geo._
import org.elasticsearch.search.sort._
import com.traackr.scalastic.elasticsearch._, SearchParameterTypes._
import scala.collection.JavaConversions._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class GeoDistanceTests extends IndexerBasedTest {
  val mapping = """{"type1": {"properties": {"location": {"type": "geo_point", "lat_lon": true}}}}"""

  test("simpleDistanceTests") {
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"name": "New York", "location": {"lat": 40.7143528, "lon": -74.0059731}}""")
    indexer.index(indexName, "type1", "2", """{"name": "Times Square", "location": {"lat": 40.759011, "lon": -73.9844722}}""")
    indexer.index(indexName, "type1", "3", """{"name": "Tribeca", "location": {"lat": 40.718266, "lon": -74.007819}}""")
    indexer.index(indexName, "type1", "4", """{"name": "Wall Street", "location": {"lat": 40.7051157, "lon": -74.0088305}}""")
    indexer.index(indexName, "type1", "5", """{"name": "Soho", "location": {"lat": 40.7247222, "lon": -74}}""")
    indexer.index(indexName, "type1", "6", """{"name": "Greenwich Village", "location": {"lat": 40.731033, "lon": -73.9962255}}""")
    indexer.index(indexName, "type1", "7", """{"name": "Brooklyn", "location": {"lat": 40.65, "lon": -73.95}}""")
    indexer.refresh()
    var response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("3km").point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (5)
    for (hit <- response.hits) Set("1", "3", "4", "5", "6") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("3km").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
    response.hits.getTotalHits should be === (5)
    response.hits.hits.length should be === (5)
    for (hit <- response.hits) Set("1", "3", "4", "5", "6") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("3km").geoDistance(GeoDistance.PLANE).point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (5)
    for (hit <- response.hits) Set("1", "3", "4", "5", "6") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("2km").point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (4)
    for (hit <- response.hits) Set("1", "3", "4", "5") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("2km").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
    response.hits.getTotalHits should be === (4)
    for (hit <- response.hits) Set("1", "3", "4", "5") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("1.242mi").point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (4)
    for (hit <- response.hits) Set("1", "3", "4", "5") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceFilter("location").distance("1.242mi").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
    response.hits.getTotalHits should be === (4)
    for (hit <- response.hits) Set("1", "3", "4", "5") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceRangeFilter("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (2)
    for (hit <- response.hits) Set("4", "5") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceRangeFilter("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
    response.hits.getTotalHits should be === (2)
    for (hit <- response.hits) Set("4", "5") should contain(hit.id)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceRangeFilter("location").to("2.0km").point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (4)
    response.hits.hits.length should be === (4)

    response = indexer.search(query = filteredQuery(matchAllQuery, geoDistanceRangeFilter("location").from("2.0km").point(40.7143528, -74.0059731)))
    response.hits.getTotalHits should be === (3)
    response.hits.hits.length should be === (3)

    response = indexer.search(sortings = Seq(GeoDistanceSort("location", geoPoint = Some(new GeoPoint(40.7143528, -74.0059731)), order = SortOrder.ASC)))
    response.hits.getTotalHits should be === (7)
    (response.hits.hits map (_.id)).toArray should be === Array("1", "3", "4", "5", "6", "2", "7")

    response = indexer.search(sortings = Seq(GeoDistanceSort("location", geoPoint = Some(new GeoPoint(40.7143528, -74.0059731)), order = SortOrder.ASC)))
    response.hits.getTotalHits should be === (7)
    (response.hits.hits map (_.id)).toArray should be === Array("1", "3", "4", "5", "6", "2", "7")
  }
}
