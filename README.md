# Scalastic 
### Scala driver for [ElasticSearch](http://www.elasticsearch.org)

Scalastic is an interface for [ElasticSearch](http://www.elasticsearch.org), designed to provide more flexible
and Scala-esque interface around the native [ElasticSearch Java API](http://www.elasticsearch.org/guide/reference/java-api/).

# Installation

Add the following to your sbt build:

```scala
libraryDependencies += "org.scalastic" %% "scalastic" % "0.90.0"
```

**Please note**, Scalastic supports Scala 2.10.x only.

# Way cool, but how do I use it?

In general, look at the [test sources](https://github.com/bsadeh/scalastic/tree/master/src/test/scala)
for usage examples.

The main dude is the `Indexer`:

```scala
import scalastic.elasticsearch._

val indexer = Indexer.<some creation method>
```

Just about every `Indexer` API call has these forms:

```scala
indexer.<api-call>          // a blocking call
indexer.send_<api-call>     // async call
indexer.prepare_<api-call>  // get the builder and tailor it all to your heart's content
```

`api-call`s employ named parameters and provide default values - you only need to provide what differs.


## Creating an Indexer (connecting to an ElasticSearch cluster)

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

## Indexing

```scala
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
indexer.putMapping(indexName, "type1", mapping)
indexer.index(indexName, "type1", "1", """{"from":"192.168.0.5", "to":"192.168.0.10"}""")
indexer.refresh()
```

* for an atomic total-reindexing operation, see `indexer.reindexWith` method
* for syncing with indexing operations on a type (index/delete), see the family of methods in the `WaitingForGodot` trait:

```scala
indexer.waitTillCount[AtLeast | Exactly | AtMost]
```

## Searching

```scala
indexer.search(query = boolQuery
    .must(rangeQuery("from") lt "192.168.0.7")
    .must(rangeQuery("to") gt "192.168.0.7"))
```

or:

```scala
val response = indexer.search(indices=List(index1, indexN), query = some_narly_query, from=100, size=25, ...)
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
* you?

# License

This software is available under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

