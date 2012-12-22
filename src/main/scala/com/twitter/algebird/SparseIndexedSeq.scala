package com.twitter.algebird

class SparseIndexedSeq[T](len : Int, sparseFactor : Double = 0.25)(implicit tMonoid : Monoid[T]) {
  val tZero = tMonoid.zero
  val sparseThreshold = len * sparseFactor

  sealed trait Base extends IndexedSeq[T] {
    val length = len
    def merge(b : Base) : Base
    def get(idx : Int) : T
    def updated(i : Int, v : T) = merge(Single(i, v))
    def apply(idx : Int) = {
      if(idx < len && idx >= 0)
        get(idx)
      else
        throw new IndexOutOfBoundsException()
    } 
  }

  object Empty extends Base {
    def get(idx : Int) = tZero
    def merge(b : Base) = b
  }

  case class Single(index : Int, value : T) extends Base {
    def get(idx : Int) = if(idx == index) value else tZero

    def merge(b : Base) : Base = {
      b match {
        case Empty => this
        case Single(i, v) => mergeSingle(i, v)
        case s:Sparse => s.mergeSingle(index, value)
        case d:Dense => d.mergeSingle(index, value)
      }
    }

    def mergeSingle(i : Int, v : T) = {
      if(i == index)
        Single(i, tMonoid.plus(v, value))
      else
        Sparse(Map(i -> v, index -> value))
    }
  }

  val mapMonoid = new MapMonoid[Int,T]
  case class Sparse(map : Map[Int,T]) extends Base {
    def get(idx : Int) = map.getOrElse(idx, tZero)

    def merge(b : Base) : Base = {
      b match {
        case Empty => this
        case Single(i, v) => mergeSingle(i,v)
        case Sparse(m) => mergeSparse(m)
        case d:Dense => d.mergeSparse(map)
      }
    }

    def mergeSingle(i : Int, v : T) = {
      val newVal = map.get(i) match {
        case Some(v2) => tMonoid.plus(v, v2)
        case None => v
      }
      sparseOrDense(map + (i -> newVal))
    }

    def mergeSparse(m : Map[Int, T]) = {
      sparseOrDense(mapMonoid.plus(map, m))
    }

    def sparseOrDense(m : Map[Int, T]) = {
      if(m.size < sparseThreshold)
        Sparse(m)
      else {
        val vector = Vector[T]().padTo(len, tZero)
        Dense(map.foldLeft(vector){ case (vec, (i,v)) => vec.updated(i, v) })
      }
    }
  }

  case class Dense(vec : Vector[T]) extends Base {
    override def apply(idx : Int) = vec(idx)
    def get(idx : Int) = vec(idx) 

    def merge(b : Base) : Base = {
      b match {
        case Empty => this
        case Single(i, v) => mergeSingle(i,v)
        case Sparse(map) => mergeSparse(map)
        case Dense(vec) => mergeDense(vec)
      }
    }

    def mergeSingle(i : Int, v : T) = {
      Dense(vec.updated(i, v))
    }

    def mergeSparse(m : Map[Int, T]) = {
      Dense(vec.zipWithIndex.map{case (v,i) => 
        m.get(i) match {
          case Some(v2) => tMonoid.plus(v, v2)
          case None => v
        }
      })
    }

    def mergeDense(other : Vector[T]) = {
      Dense(vec.zip(other).map{case (v1, v2) => tMonoid.plus(v1, v2)})
    }
  }

  val monoid = new Monoid[Base] {
    def zero = Empty
    def plus(a : Base, b : Base) : Base = a.merge(b)
  }
}