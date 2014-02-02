# Scalastic 
### Scala driver for [ElasticSearch](http://www.elasticsearch.org)

Scalastic is an interface for [ElasticSearch](http://www.elasticsearch.org), designed to provide more flexible
and Scala-esque interface around the native [ElasticSearch Java API](http://www.elasticsearch.org/guide/reference/java-api/).

# Installation

Add the following to your sbt build:

```scala
libraryDependencies += "org.scalastic" %% "scalastic" % "0.90.10"
```

**Please note**, Scalastic supports Scala 2.10.x only.

# Way cool, but how do I use it?

In general, look at the [test sources](https://github.com/bsadeh/scalastic/tree/master/src/test/scala)
for usage examples.

## Connect to an ElasticSearch cluster

The main dude is the `Indexer`:

```scala
import scalastic.elasticsearch._

val indexer = Indexer.<some creation method>
```

### Using node-based access:

```scala
val indexer = Indexer.local.start
```


```scala
val indexer = Indexer.using(settings) // String or Map
```


```scala
val indexer = Indexer.at(node)
```

### Using a transport client:

```scala
val indexer = Indexer.transport(settings = Map(...), host = "...")
```

## General API structure

Just about every `Indexer` API call has these forms:

```scala
indexer.<api-call>          // a blocking call
indexer.<api-call>_send     // async call
indexer.<api-call>_prepare  // get the builder and tailor it all to your heart's content
```

`api-call`s employ named parameters and provide default values - you only need to provide what differs.


## Indexing

```scala
val indexType = "subnet"

val mapping = s"""
{
    "$indexType": {
        "properties" : {
            "from" : {"type": "ip"},
            "to" : {"type": "ip"}
        }
    }
}
"""
val indexName = "networks"

indexer.createIndex(indexName, settings = Map("number_of_shards" -> "1"))
indexer.waitTillActive()

indexer.putMapping(indexName, indexType, mapping)

indexer.index(indexName, indexType, "1", """{"from":"192.168.0.5", "to":"192.168.0.10"}""")

indexer.refresh()
```

* for an atomic total-reindexing operation, see `indexer.reindexWith` method
* for syncing with indexing operations on a type (index/delete), see the family of methods in the `WaitingForGodot` trait:

```scala
indexer.waitTillCount[AtLeast | Exactly | AtMost]
```

## Searching

```scala
import org.elasticsearch.index.query.QueryBuilders._

indexer.search(query = boolQuery
    .must(rangeQuery("from") lt "192.168.0.7")
    .must(rangeQuery("to") gt "192.168.0.7"))
```

or:

```scala
import org.elasticsearch.index.query.QueryBuilder

val searchQuery: QueryBuilder = ...
val response = indexer.search(indices = List("index1", "indexN"),
                              query = searchQuery,
                              from = 100,
                              size = 25 /* and so on */)
```

## Testing
Try mixing in the `UsingIndexer` trait

## Building from source
* Scala 2.10
* sbt 0.12.3

## Versioning scheme
Scalastic versions correspond to ElasticSearch versions (starting from `0.90.0` binaries are available via Maven repo)
with a small addition - *the fourth* component of the version is used to reflect Scalastic improvements/bug fixes.

For example:
given ElasticSearch 0.90.0 - Scalastic versions will be 0.90.0, 0.90.0.1, 0.90.0.2 and so on.

## Contributors
* Benny Sadeh <benny.sadeh@gmail.com>
* Ivan Yatskevich ([github](https://github.com/yatskevich))
* you?

# License

This software is available under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

