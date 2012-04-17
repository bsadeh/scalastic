package com.traackr.elasticsearch

trait UsingIndexer {
  var indexer: Indexer = _

  def createIndexer = Indexer.local

  def indexName = getClass.getSimpleName.toLowerCase

  def indexer_beforeAll = { indexer = createIndexer.start }

  def indexer_beforeEach = indexer.deleteIndex()
  
  def indexer_afterAll = indexer.stop

}