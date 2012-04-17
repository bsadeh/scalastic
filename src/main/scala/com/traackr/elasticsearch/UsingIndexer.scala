package com.traackr.elasticsearch

trait UsingIndexer {
  var indexer: Indexer = _

  def createIndexer = Indexer.local

  def indexName = getClass.getSimpleName().toLowerCase

  def indexer_beforeAll = { indexer = createIndexer.start }

  def indexer_beforeEach = indexer_discardIndex
  
  def indexer_afterEach = indexer_discardIndex

  def indexer_afterAll = indexer.stop

  def indexer_discardIndex = if (indexer.exists(indexName)) {
    indexer.deleteIndex(indexName)
    indexer.waitForGreenStatus()
  }

}