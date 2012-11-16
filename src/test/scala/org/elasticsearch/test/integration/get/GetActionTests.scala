package org.elasticsearch.test.integration.get

import org.elasticsearch.action.admin.cluster.health._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class GetActionTests extends IndexerBasedTest {

  test("simpleGetTests") {
    var response = indexer.get(indexName, "type1", "1")
    response.exists() should be === (false)
    //logger.info("--> index doc 1")
    indexer.index(indexName, "type1", "1", """{"field1": "value1", "field2": "value2"}""")
    //logger.info("--> realtime get 1")
    response = indexer.get(indexName, "type1", "1")
    response.exists() should be === (true)
    response.sourceAsMap().get("field1").toString should be === ("value1")
    response.sourceAsMap().get("field2").toString should be === ("value2")

    //logger.info("--> realtime get 1 (no type)")
    response = indexer.get(indexName, null, "1")
    response.exists() should be === (true)
    response.sourceAsMap().get("field1").toString should be === ("value1")
    response.sourceAsMap().get("field2").toString should be === ("value2")
    //logger.info("--> non realtime get 1")
    response = indexer.get(indexName, "type1", "1", realtime = Some(false))
    response.exists() should be === (false)
    //logger.info("--> realtime fetch of field (requires fetching parsing source)")
    response = indexer.get(indexName, "type1", "1", fields = Seq("field1"))
    response.exists() should be === (true)
    response.source() should be(null)
    response.field("field1").values.get(0).toString should be === ("value1")
    response.field("field2") should be(null)
    //logger.info("--> flush the index, so we load it from it")
    indexer.flush()
    //logger.info("--> realtime get 1 (loaded from index)")
    response = indexer.get(indexName, "type1", "1")
    response.exists() should be === (true)
    response.sourceAsMap().get("field1").toString should be === ("value1")
    response.sourceAsMap().get("field2").toString should be === ("value2")
    //logger.info("--> non realtime get 1 (loaded from index)")
    response = indexer.get(indexName, "type1", "1", realtime = Some(false))
    response.exists() should be === (true)
    response.sourceAsMap().get("field1").toString should be === ("value1")
    response.sourceAsMap().get("field2").toString should be === ("value2")
    //logger.info("--> realtime fetch of field (loaded from index)")
    response = indexer.get(indexName, "type1", "1", fields = Seq("field1"))
    response.exists() should be === (true)
    response.source() should be(null)
    response.field("field1").values.get(0).toString should be === ("value1")
    response.field("field2") should be(null)
    //logger.info("--> update doc 1")
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1", "field2": "value2_1"}""")
    //logger.info("--> realtime get 1")
    response = indexer.get(indexName, "type1", "1")
    response.exists() should be === (true)
    response.sourceAsMap().get("field1").toString should be === ("value1_1")
    response.sourceAsMap().get("field2").toString should be === ("value2_1")
    //logger.info("--> update doc 1 again")
    indexer.index(indexName, "type1", "1", """{"field1": "value1_2", "field2": "value2_2"}""")
    response = indexer.get(indexName, "type1", "1")
    response.exists() should be === (true)
    response.sourceAsMap().get("field1").toString should be === ("value1_2")
    response.sourceAsMap().get("field2").toString should be === ("value2_2")
    val deleteResponse = indexer.delete(indexName, "type1", "1")
    deleteResponse.notFound() should be === (false)
    response = indexer.get(indexName, "type1", "1")
    response.exists() should be === (false)
  }

  test("simpleMultiGetTests") {
    var response = indexer.multiget(indexName, "type1", Seq("1"))
    response.responses().length should be === (1)
    response.responses()(0).response().exists() should be === (false)
    for (i <- 0 until 10) indexer.index(indexName, "type1", i.toString, """{"field": "value%s"}""".format(i))
    response = indexer.multiget(indexName, "type1", Seq("1", "15", "3", "9", "11"))
    response.responses().length should be === (5)
    (response.responses() map (_.id)) should be === Array("1", "15", "3", "9", "11")
    (response.responses() map (_.response.exists)) should be === Array(true, false, true, true, false)
    response.responses()(0).response().sourceAsMap().get("field").toString should be === ("value1")

    response = indexer.multiget(indexName, "type1", ids = Seq("1", "3"), fields = Seq("field"))
    response.responses().length should be === (2)
    response.responses()(0).response().source() should be(null)
    response.responses()(0).response().field("field").values.get(0).toString should be === ("value1")
  }

  test("realtimeGetWithCompress") {
    indexer.putMapping(indexName, "type", """{"type": {"_source": {"compress": true}}}""")
    val sb = new StringBuilder()
    for (i <- 0 until 1000) sb.append(java.util.UUID.randomUUID().toString)
    val fieldValue = sb.toString
    fieldValue.size should be === (36 * 1000)
    indexer.index(indexName, "type", "1", """{"field": "%s"}""".format(fieldValue))
    val getResponse = indexer.get(indexName, "type", "1")
    getResponse.exists() should be === (true)
    getResponse.sourceAsMap().get("field").toString should be === (fieldValue)
  }
}
