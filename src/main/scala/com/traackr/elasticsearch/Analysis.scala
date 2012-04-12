package com.traackr.elasticsearch

trait Analysis {
  self: Indexer =>
  def analyze(text: String, index: String = null, field: String = null, tokenizer: String = null, tokenFilters: Seq[String] = Seq(), analyzer: String = null) = analyze_send(text, index, field, tokenizer, tokenFilters, analyzer).actionGet
  def analyze_send(text: String, index: String = null, field: String = null, tokenizer: String = null, tokenFilters: Seq[String] = Seq(), analyzer: String = null) = analyze_prepare(text, index, field, tokenizer, tokenFilters, analyzer).execute
  def analyze_prepare(text: String, index: String = null, field: String = null, tokenizer: String = null, tokenFilters: Seq[String] = Seq(), analyzer: String = null) = {
    val request = client.admin.indices.prepareAnalyze(text)
    if (index != null) {
      request.setIndex(index)
      if (field != null) request.setField(field)
    }
    if (tokenizer != null) {
      request.setTokenizer(tokenizer)
      request.setTokenFilters(tokenFilters.toArray: _*)
    }
    if (analyzer != null) request.setAnalyzer(analyzer)
    request
  }
}
