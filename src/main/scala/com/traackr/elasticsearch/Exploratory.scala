package com.traackr.elasticsearch

import org.elasticsearch.client._
import org.elasticsearch.action._

trait Exploratory {
  def client: Client
  
  type Request <: ActionRequest
  type Response <: ActionResponse
  type Builder <: ActionRequestBuilder[Request, Response]
  type Listenable <: ListenableActionFuture[Response]

  def execute[Builder <: ActionRequestBuilder[Request, Response]](client: Client)(f: Client => Builder): ListenableActionFuture[Response] = f(client).execute
  def get(builder: Builder)(f: Builder => Listenable): Response = f(builder).actionGet
}