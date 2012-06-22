package com.traackr.scalastic.elasticsearch

import org.scalatest._, matchers._
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.search._
import org.elasticsearch.common.collect._
import org.elasticsearch.search.sort._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleFieldsTest extends IndexerBasedTest {

  override def beforeEach {
    super.beforeEach
    createDefaultIndex
  }

  test("stored fields") {
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
    var response = indexer.search(fields = List("field1"))
    response.hits.getTotalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).fields.size should be === (1)
    response.hits.getAt(0).fields.get("field1").value.toString should be === ("value1")

        // field2 is not stored, check that it gets extracted from source
    response = indexer.search(fields = List("field2"))
    response.hits.getTotalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).fields.size should be === (1)
    response.hits.getAt(0).fields.get("field2").value.toString should be === ("value2")

    response = indexer.search(fields = List("field3"))
    response.hits.getTotalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).fields.size should be === (1)
    response.hits.getAt(0).fields.get("field3").value.toString should be === ("value3")

    response = indexer.search(fields = List("*"))
    response.hits.getTotalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).source should be (null)
    response.hits.getAt(0).fields.size should be === (2)
    response.hits.getAt(0).fields.get("field1").value.toString should be === ("value1")
    response.hits.getAt(0).fields.get("field3").value.toString should be === ("value3")

    response = indexer.search(fields = List("*", "_source"))
    response.hits.getTotalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).source should not be (null)
    response.hits.getAt(0).fields.size should be === (2)
    response.hits.getAt(0).fields.get("field1").value.toString should be === ("value1")
    response.hits.getAt(0).fields.get("field3").value.toString should be === ("value3")
  }

  test("script doc & fields") {
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
      ("sNum1", "doc['num1'].value", null),
      ("sNum1_field", "_fields['num1'].value", null),
      ("date1", "doc['date'].date.millis", null))
    var response = indexer.search(scriptFields = scripts, sorting = List(FieldSortSpec("num1")))
    response.shardFailures.length should be === (0)
    response.hits.totalHits should be === (3)
    response.hits.getAt(0).isSourceEmpty should be(true)
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(0).fields.get("sNum1").values.get(0) should be === (1.0)
    response.hits.getAt(0).fields.get("sNum1_field").values.get(0) should be === (1.0)
    response.hits.getAt(0).fields.get("date1").values.get(0) should be === (0)
    response.hits.getAt(1).id should be === ("2")
    response.hits.getAt(1).fields.get("sNum1").values.get(0) should be === (2.0)
    response.hits.getAt(1).fields.get("sNum1_field").values.get(0) should be === (2.0)
    response.hits.getAt(1).fields.get("date1").values.get(0) should be === (25000)
    response.hits.getAt(2).id should be === ("3")
    response.hits.getAt(2).fields.get("sNum1").values.get(0) should be === (3.0)
    response.hits.getAt(2).fields.get("sNum1_field").values.get(0) should be === (3.0)
    response.hits.getAt(2).fields.get("date1").values.get(0) should be === (120000)

    val params: Map[String, Object]= Map("factor" -> new java.lang.Double(2.0))
    response = indexer.search(scriptFields = Seq(Tuple3("sNum1","doc['num1'].value * factor", params)), sorting = List(FieldSortSpec("num1")))
    response.hits.totalHits should be === (3)
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(0).fields.get("sNum1").values.get(0) should be === (2.0)
    response.hits.getAt(1).id should be === ("2")
    response.hits.getAt(1).fields.get("sNum1").values.get(0) should be === (4.0)
    response.hits.getAt(2).id should be === ("3")
    response.hits.getAt(2).fields.get("sNum1").values.get(0) should be === (6.0)
  }

  test("partial fields") {
    import java.util.{ Map => JMap, List => JList }
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
    val response = indexer.search(partialFields=Seq(("partial1", Seq("obj1.arr1.*"), Seq()), ("partial2", Seq(), Seq("obj1.*"))))
    response.shardFailures.length should be === (0)
    
    val partial1 = response.hits.getAt(0).field("partial1").value.asInstanceOf[JMap[String, _]]
    partial1 should not contain key ("field1")
    partial1 should contain key ("obj1")
    partial1.get("obj1").asInstanceOf[JMap[String, _]].get("arr1").asInstanceOf[JList[_]] should not be ('empty)
    
    val partial2 = response.hits.getAt(0).field("partial2").value.asInstanceOf[JMap[String, _]]
    partial2 should not contain key ("obj1")
    partial2 should contain key ("field1")
  }
}
