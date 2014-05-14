package scalastic.elasticsearch

trait UsingIndexer {
  implicit val indexer: Indexer = createIndexer

  def createIndexer = Indexer.local
  def indexName = getClass.getSimpleName.toLowerCase
  def indexer_beforeAll = indexer.start
  def indexer_beforeEach = indexer.deleteIndex(Set("_all"))
  def indexer_afterAll = indexer.stop
}