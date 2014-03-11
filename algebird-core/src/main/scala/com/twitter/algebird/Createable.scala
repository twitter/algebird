package com.twitter.algebird

/**
  * Factory trait for creating instances of O.
  */
trait Creatable[I, O] {
  def createCreatable(i: I): O = batchCreateCreatable(Seq(i))
  def batchCreateCreatable(is: Seq[I]): O
}
