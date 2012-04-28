package com.traackr.elasticsearch

import scalaz._, Scalaz._

trait Analysis {
  self: Indexer =>
  def analyze(text: String, index: Option[String] = None, field: Option[String] = None, tokenizer: Option[String] = None, tokenFilters: Iterable[String] = Nil, analyzer: Option[String] = None) = analyze_send(text, index, field, tokenizer, tokenFilters, analyzer).actionGet
  def analyze_send(text: String, index: Option[String] = None, field: Option[String] = None, tokenizer: Option[String] = None, tokenFilters: Iterable[String] = Nil, analyzer: Option[String] = None) = analyze_prepare(text, index, field, tokenizer, tokenFilters, analyzer).execute
  def analyze_prepare(text: String, index: Option[String] = None, field: Option[String] = None, tokenizer: Option[String] = None, tokenFilters: Iterable[String] = Nil, analyzer: Option[String] = None) = {
    val request = client.admin.indices.prepareAnalyze(text)
    index some { that =>
      request.setIndex(that)
      field some { request.setField(_) }
    }
    tokenizer some { that =>
      request.setTokenizer(that)
      request.setTokenFilters(tokenFilters.toArray: _*)
    }
    analyzer some { request.setAnalyzer(_) }
    request
  }
}
