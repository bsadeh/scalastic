package org.elasticsearch.test.integration.search.facet


/** Tests for several shards case since some facets do optimizations in this case. Make sure
 *  behavior remains the same.
 */
@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleFacetsMultiShardTests extends SimpleFacetsTests {

  protected override def numberOfShards(): Int = 3
}
