package org.elasticsearch.test.integration.search.scriptfilter

import org.scalatest._, matchers._
import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.search.sort._
import com.traackr.scalastic.elasticsearch._, SearchParameterTypes._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ScriptFilterSearchTests extends IndexerBasedTest {

  override def defaultSettings = Map("number_of_shards" -> "1", "number_of_replicas" -> "0")

  test("customScriptBoost") {
    indexer.index(indexName, "type1", "1", """{"test": "value beck", "num1": 1.0}""")
    indexer.index(indexName, "type1", "2", """{"test": "value beck", "num1": 2.0}""")
    indexer.index(indexName, "type1", "3", """{"test": "value beck", "num1": 3.0}""")
    indexer.refresh()

    //info("running doc['num1'].value > 1")
    var response = indexer.search(
      query = filteredQuery(matchAllQuery, scriptFilter("doc['num1'].value > 1")),
      sortings = Seq(FieldSort("num1", order = SortOrder.ASC)),
      scriptFields = Seq(ScriptField("sNum1", "doc['num1'].value")))
    response.hits.totalHits should be === (2)
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(0).fields().get("sNum1").values.get(0).asInstanceOf[java.lang.Double] should be === (2.0)
    response.hits.getAt(1).id should be === ("3")
    response.hits.getAt(1).fields().get("sNum1").values.get(0).asInstanceOf[java.lang.Double] should be === (3.0)

    //info("running doc['num1'].value > param1")
    response = indexer.search_prepare().setQuery(filteredQuery(matchAllQuery, scriptFilter("doc['num1'].value > param1").addParam("param1",
      2)))
      .addSort("num1", SortOrder.ASC)
      .addScriptField("sNum1", "doc['num1'].value").execute.actionGet
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("3")
    response.hits.getAt(0).fields().get("sNum1").values.get(0).asInstanceOf[java.lang.Double] should be === (3.0)

    //info("running doc['num1'].value > param1")
    response = indexer.search_prepare().setQuery(filteredQuery(matchAllQuery, scriptFilter("doc['num1'].value > param1").addParam("param1",
      -1)))
      .addSort("num1", SortOrder.ASC)
      .addScriptField("sNum1", "doc['num1'].value").execute.actionGet
    response.hits.totalHits should be === (3)
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(0).fields().get("sNum1").values.get(0).asInstanceOf[java.lang.Double] should be === (1.0)
    response.hits.getAt(1).id should be === ("2")
    response.hits.getAt(1).fields().get("sNum1").values.get(0).asInstanceOf[java.lang.Double] should be === (2.0)
    response.hits.getAt(2).id should be === ("3")
    response.hits.getAt(2).fields().get("sNum1").values.get(0).asInstanceOf[java.lang.Double] should be === (3.0)
  }
}
