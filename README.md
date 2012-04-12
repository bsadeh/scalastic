# Scalastic 
### Scala driver for [ElasticSearch](http://www.elasticsearch.org)

Contributors
---
* Benny Sadeh <benny.sadeh@gmail.com> 
* you?

About
---
Scalastic is an interface for [ElasticSearch](http://www.elasticsearch.org), designed to provide more flexible and Scala-esque interface around the native [elasticsearch Java API](http://www.elasticsearch.org/guide/reference/java-api/).

Scalastic has been developed and tested against elasticsearch 1.9.x+.


Way cool, but how do I use it?
---
in general, look at the scalatest source for usage examples ...

the main dude is the Indexer:
	import com.traackr.elasticsearch._
	val indexer = Indexer.<some creation method>

just about every Indexer api call has these forms:
	indexer.<api-call>			// a blocking call
	indexer.send_<api-call>		// async call
	indexer.prepare_<api-call>	// get the builder and tailor it all to your heart's content

api-calls employ named parameters and provide default values;
you only need to provide what differs.


## Creating an Indexer (connecting to an elastic cluster)
using node-based access:

	val indexer = Indexer.local.start
	val indexer = Indexer.using(settings) // String or Map
	val indexer = Indexer.at(node)

using a transport client:
	val indexer = Indexer.transport(settings = Map(...), host = "...")

## Indexing
    val mapping = """
    {
	  "type1": {
	    "properties" : {
		  "from" : {"type": "ip"},
    		  "to" : {"type": "ip"}		
	    }
	  }
    }
    """
    indexer.createIndex("index1", settings = """{"number_of_shards":1}""")
    indexer.waitTillActive()
    indexer.putMapping(indexName, "type9", mapping)
    indexer.index(indexName, "type9", "1", """{"from":"192.168.0.5", "to":"192.168.0.10"}""")
    indexer.refresh()

## Searching
    indexer.search(query = boolQuery
      .must(rangeQuery("from") lt "192.168.0.7")
      .must(rangeQuery("to") gt "192.168.0.7"))
or:
    val response = indexer.search(indices=List(index1, indexN), query = some_narly_query, from=100, size=25, etc. etc.)

## Testing
* try mixing in the UsingIndexer trait

# Building
* sbt 0.11.2
* Maven 3.0.4

