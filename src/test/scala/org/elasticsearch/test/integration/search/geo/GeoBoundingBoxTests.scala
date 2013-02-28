package org.elasticsearch.test.integration.search.geo

import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import scala.collection.JavaConversions._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class GeoBoundingBoxTests extends IndexerBasedTest {
  val mapping = """{"type1": {"properties": {"location": {"type": "geo_point", "lat_lon": true}}}}"""

  test("simpleBoundingBoxTest") {
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"name": "New York", "location": {"lat": 40.7143528, "lon": -74.0059731}}""")
    indexer.index(indexName, "type1", "2", """{"name": "Times Square", "location": {"lat": 40.759011, "lon": -73.9844722}}""")
    indexer.index(indexName, "type1", "3", """{"name": "Tribeca", "location": {"lat": 40.718266, "lon": -74.007819}}""")
    indexer.index(indexName, "type1", "4", """{"name": "Wall Street", "location": {"lat": 40.7051157, "lon": -74.0088305}}""")
    indexer.index(indexName, "type1", "5", """{"name": "Soho", "location": {"lat": 40.7247222, "lon": -74}}""")
    indexer.index(indexName, "type1", "6", """{"name": "Greenwich Village", "location": {"lat": 40.731033, "lon": -73.9962255}}""")
    indexer.index(indexName, "type1", "7", """{"name": "Brooklyn", "location": {"lat": 40.65, "lon": -73.95}}""")
    indexer.refresh()
    var response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(40.73, -74.1).bottomRight(40.717, -73.99)))
    response.getHits.getTotalHits should be === (2)
    response.getHits.hits.length should be === (2)
    for (hit <- response.getHits) Set("1", "3", "5") should contain(hit.getId)

    response = indexer.search_prepare().setQuery(filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(40.73,
      -74.1)
      .bottomRight(40.717, -73.99)
      .`type`("indexed"))).execute.actionGet
    response.getHits.getTotalHits should be === (2)
    response.getHits.hits.length should be === (2)
    for (hit <- response.getHits) Set("1", "3", "5") should contain(hit.getId)
  }

  test("limitsBoundingBoxTest") {
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"location": {"lat": 40, "lon": -20}}""")
    indexer.index(indexName, "type1", "2", """{"location": {"lat": 40, "lon": -10}}""")
    indexer.index(indexName, "type1", "3", """{"location": {"lat": 40, "lon": 10}}""")
    indexer.index(indexName, "type1", "4", """{"location": {"lat": 40, "lon": 20}}""")
    indexer.index(indexName, "type1", "5", """{"location": {"lat": 10, "lon": -170}}""")
    indexer.index(indexName, "type1", "6", """{"location": {"lat": 0, "lon": -170}}""")
    indexer.index(indexName, "type1", "7", """{"location": {"lat": -10, "lon": -170}}""")
    indexer.index(indexName, "type1", "8", """{"location": {"lat": 10, "lon": 170}}""")
    indexer.index(indexName, "type1", "9", """{"location": {"lat": 0, "lon": 170}}""")
    indexer.index(indexName, "type1", "10", """{"location": {"lat": -10, "lon": 170}}""")
    indexer.refresh()

    var response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(41, -11).bottomRight(40, 9)))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("2")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(41, -11).bottomRight(40, 9).`type`("indexed")))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("2")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(41, -9).bottomRight(40, 11)))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("3")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(41, -9).bottomRight(40, 11).`type`("indexed")))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("3")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(11, 171).bottomRight(1, -169)))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("5")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(11, 171).bottomRight(1, -169).`type`("indexed")))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("5")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(9, 169).bottomRight(-1, -171)))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("9")

    response = indexer.search(query = filteredQuery(matchAllQuery, geoBoundingBoxFilter("location").topLeft(9, 169).bottomRight(-1, -171).`type`("indexed")))
    response.getHits.getTotalHits should be === (1)
    response.getHits.hits.length should be === (1)
    response.getHits.getAt(0).id should be === ("9")
  }

  test("limit2BoundingBoxTest") {
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"userid": 880, "title": "Place in Stockholm", "location": {"lat": 59.328355000000002, "lon": 18.036842}}""")
    indexer.index(indexName, "type1", "2", """{"userid": 534, "title": "Place in Montreal", "location": {"lat": 45.509526999999999, "lon": -73.570986000000005}}""")
    indexer.refresh()
    var response = indexer.search(query = filteredQuery(termQuery("userid", 880), geoBoundingBoxFilter("location").topLeft(74.579421999999994, 143.5).bottomRight(-66.668903999999998, 113.96875)))
    response.getHits.totalHits should be === (1)
    response = indexer.search_prepare().setQuery(filteredQuery(termQuery("userid", 880), geoBoundingBoxFilter("location").topLeft(74.579421999999994,
      143.5)
      .bottomRight(-66.668903999999998, 113.96875)
      .`type`("indexed"))).execute.actionGet
    response.getHits.totalHits should be === (1)
    response = indexer.search(query = filteredQuery(termQuery("userid", 534), geoBoundingBoxFilter("location").topLeft(74.579421999999994, 143.5).bottomRight(-66.668903999999998, 113.96875)))
    response.getHits.totalHits should be === (1)
    response = indexer.search_prepare().setQuery(filteredQuery(termQuery("userid", 534), geoBoundingBoxFilter("location").topLeft(74.579421999999994,
      143.5)
      .bottomRight(-66.668903999999998, 113.96875)
      .`type`("indexed"))).execute.actionGet
    response.getHits.totalHits should be === (1)
  }
}
