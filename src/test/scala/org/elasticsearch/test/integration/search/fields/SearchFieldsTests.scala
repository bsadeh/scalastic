package org.elasticsearch.test.integration.search.fields

import org.elasticsearch.index.query.QueryBuilders._
import scalastic.elasticsearch._, SearchParameterTypes._
import java.util.{ Map => JMap, List => JList }

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class SearchFieldsTests extends IndexerBasedTest {

  test("testStoredFields") {
    val mapping =
    """
	{
	  "type":{
	    "properties":{
		    "field1":{"type":"string", "store":"yes"},
			"field2":{"type":"string", "store":"no"},
	        "field3":{"type":"string", "store":"yes"}
	    }
	  }
	}
    """
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"field1": "value1","field2": "value2","field3": "value3"}""")
    indexer.refresh()
    var response = indexer.search(fields = Seq("field1"))
    response.getHits.getTotalHits should equal (1)
    response.getHits.hits.length should equal (1)
    response.getHits.getAt(0).fields.size should equal (1)
    response.getHits.getAt(0).fields.get("field1").value.toString should equal ("value1")

    // field2 is not stored, check that it gets extracted from source
    response = indexer.search(fields = Seq("field2"))
    response.getHits.getTotalHits should equal (1)
    response.getHits.hits.length should equal (1)
    response.getHits.getAt(0).fields.size should equal (1)
    response.getHits.getAt(0).fields.get("field2").value.toString should equal ("value2")

    response = indexer.search(fields = Seq("field3"))
    response.getHits.getTotalHits should equal (1)
    response.getHits.hits.length should equal (1)
    response.getHits.getAt(0).fields.size should equal (1)
    response.getHits.getAt(0).fields.get("field3").value.toString should equal ("value3")

    response = indexer.search(fields = Seq("*"))
    response.getHits.getTotalHits should equal (1)
    response.getHits.hits.length should equal (1)
    response.getHits.getAt(0).source should be(null)
    response.getHits.getAt(0).fields.size should equal (2)
    response.getHits.getAt(0).fields.get("field1").value.toString should equal ("value1")
    response.getHits.getAt(0).fields.get("field3").value.toString should equal ("value3")

    response = indexer.search(fields = Seq("*", "_source"))
    response.getHits.getTotalHits should equal (1)
    response.getHits.hits.length should equal (1)
    response.getHits.getAt(0).source should not be (null)
    response.getHits.getAt(0).fields.size should equal (2)
    response.getHits.getAt(0).fields.get("field1").value.toString should equal ("value1")
    response.getHits.getAt(0).fields.get("field3").value.toString should equal ("value3")
  }

  test("testScriptDocAndFields") {
    val mapping =
    """
	{
	  "type":{
	    "properties":{
	        "num1":{"type":"double", "store":"yes"}
	    }
	  }
	}
    """
    indexer.putMapping(indexName, "type1", mapping)
    indexer.index(indexName, "type1", "1", """{"test": "value beck", "num1": 1.0, "date": "1970-01-01T00:00:00"}""")
    indexer.index(indexName, "type1", "2", """{"test": "value beck", "num1": 2.0, "date": "1970-01-01T00:00:25"}""")
    indexer.index(indexName, "type1", "3", """{"test": "value beck", "num1": 3.0, "date": "1970-01-01T00:02:00"}""")
    indexer.refresh()

    val scripts = Seq(
      ScriptField("sNum1", "doc['num1'].value"),
      ScriptField("sNum1_field", "_fields['num1'].value"),
      ScriptField("date1", "doc['date'].date.millis"))
    var response = indexer.search(scriptFields = scripts, sortings = Seq(FieldSort("num1")))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (3)
    response.getHits.getAt(0).isSourceEmpty should be(true)
    response.getHits.getAt(0).id should equal ("1")
    response.getHits.getAt(0).fields.get("sNum1").values.get(0) should equal (1.0)
    response.getHits.getAt(0).fields.get("sNum1_field").values.get(0) should equal (1.0)
    response.getHits.getAt(0).fields.get("date1").values.get(0) should equal (0)
    response.getHits.getAt(1).id should equal ("2")
    response.getHits.getAt(1).fields.get("sNum1").values.get(0) should equal (2.0)
    response.getHits.getAt(1).fields.get("sNum1_field").values.get(0) should equal (2.0)
    response.getHits.getAt(1).fields.get("date1").values.get(0) should equal (25000)
    response.getHits.getAt(2).id should equal ("3")
    response.getHits.getAt(2).fields.get("sNum1").values.get(0) should equal (3.0)
    response.getHits.getAt(2).fields.get("sNum1_field").values.get(0) should equal (3.0)
    response.getHits.getAt(2).fields.get("date1").values.get(0) should equal (120000)

    val parameters = Map("factor" -> new java.lang.Double(2.0))
    response = indexer.search(scriptFields = Seq(ScriptField("sNum1", "doc['num1'].value * factor", parameters)), sortings = Seq(FieldSort("num1")))
    response.getHits.totalHits should equal (3)
    response.getHits.getAt(0).id should equal ("1")
    response.getHits.getAt(0).fields.get("sNum1").values.get(0) should equal (2.0)
    response.getHits.getAt(1).id should equal ("2")
    response.getHits.getAt(1).fields.get("sNum1").values.get(0) should equal (4.0)
    response.getHits.getAt(2).id should equal ("3")
    response.getHits.getAt(2).fields.get("sNum1").values.get(0) should equal (6.0)
  }

  test("testScriptFieldUsingSource") {
    indexer.index(indexName, "type1", "1", """{"obj1": {"test": "something"}, obj2: {"arr2": ["arr_value1", "arr_value2"]}, "arr3": [{"arr3_field1": "arr3_value1"}] }""")
    indexer.refresh()

    val response = indexer.search_prepare().setQuery(matchAllQuery).addField("_source.obj1")
      .addScriptField("s_obj1", "_source.obj1")
      .addScriptField("s_obj1_test", "_source.obj1.test")
      .addScriptField("s_obj2", "_source.obj2")
      .addScriptField("s_obj2_arr2", "_source.obj2.arr2")
      .addScriptField("s_arr3", "_source.arr3").execute.actionGet
    response.getShardFailures.length should equal (0)
    
    var sObj1 = response.getHits.getAt(0).field("_source.obj1").value.asInstanceOf[JMap[String, _]]
    sObj1.get("test").toString should equal ("something")
    response.getHits.getAt(0).field("s_obj1_test").value().toString should equal ("something")
    sObj1 = response.getHits.getAt(0).field("s_obj1").value()
    sObj1.get("test").toString should equal ("something")
    response.getHits.getAt(0).field("s_obj1_test").value().toString should equal ("something")

    val sObj2 = response.getHits.getAt(0).field("s_obj2").value.asInstanceOf[JMap[String, _]]
    var sObj2Arr2 = sObj2.get("arr2").asInstanceOf[JList[_]]
    sObj2Arr2.size should equal (2)
    sObj2Arr2.get(0).toString should equal ("arr_value1")
    sObj2Arr2.get(1).toString should equal ("arr_value2")
    sObj2Arr2 = response.getHits.getAt(0).field("s_obj2_arr2").value().asInstanceOf[JList[_]]
    sObj2Arr2.size should equal (2)
    sObj2Arr2.get(0).toString should equal ("arr_value1")
    sObj2Arr2.get(1).toString should equal ("arr_value2")
    val sObj2Arr3 = response.getHits.getAt(0).field("s_arr3").value().asInstanceOf[JList[_]]
    sObj2Arr3.get(0).asInstanceOf[JMap[String, _]].get("arr3_field1").toString should equal ("arr3_value1")
  }

  test("testPartialFields") {
    val json =
    """
	{
	   "field1":"value1",
	   "obj1":{
	      "arr1":[
	         {"obj2":{"field2":"value21"}},
	         {"obj2":{"field2":"value22"}}
	      ]
	   }
	}	      
	"""
    indexer.index(indexName, "type1", "1", json)
    indexer.refresh()
    val response = indexer.search(partialFields = Seq(PartialField("partial1", Some("obj1.arr1.*"), None), PartialField("partial2", None, Some("obj1.*"))))
    response.getShardFailures.length should equal (0)

    val partial1 = response.getHits.getAt(0).field("partial1").value.asInstanceOf[JMap[String, _]]
    partial1 should not contain key("field1")
    partial1 should contain key ("obj1")
    partial1.get("obj1").asInstanceOf[JMap[String, _]].get("arr1").asInstanceOf[JList[_]] should not be ('empty)

    val partial2 = response.getHits.getAt(0).field("partial2").value.asInstanceOf[JMap[String, _]]
    partial2 should not contain key("obj1")
    partial2 should contain key ("field1")
  }
}
