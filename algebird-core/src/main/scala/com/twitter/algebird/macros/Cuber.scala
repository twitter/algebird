package com.twitter.algebird.macros

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

// import com.twitter.algebird._

trait Cuber[I] {
  type K
  def apply(in: I): TraversableOnce[K]
}

trait Roller[I] {
  type K
  def apply(in: I): TraversableOnce[K]
}

object Cuber {
  def cuber[T]: Cuber[T] = macro cuberImpl[T]

  def cuberImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Cuber[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val params = getParams(c)
    if (params.length > 22)
      c.abort(c.enclosingPosition, s"Cannot create Cuber for $T because it has more than 22 parameters.")
    if (params.length == 0)
      c.abort(c.enclosingPosition, s"Cannot create Cuber for $T because it has no parameters.")

    val tupleName = newTypeName(s"Tuple${params.length}")
    val types = params.map { param => tq"_root_.scala.Option[${param.returnType}]" }

    val fors = params.map { param =>
      fq"""${param.name.asInstanceOf[c.TermName]} <- _root_.scala.Seq(_root_.scala.Some(in.${param}), _root_.scala.None)"""
    }
    val names = params.map { param => param.name.asInstanceOf[c.TermName] }

    val cuber = q"""
    new _root_.com.twitter.algebird.macros.Cuber[${T}] {
      type K = $tupleName[..$types]
      def apply(in: ${T}): _root_.scala.Seq[K] = for (..$fors) yield new K(..$names)
    }
    """
    c.Expr[Cuber[T]](cuber)
  }
}

object Roller {
  def roller[T]: Roller[T] = macro rollerImpl[T]

  def rollerImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Roller[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val params = getParams(c)
    if (params.length > 22)
      c.abort(c.enclosingPosition, s"Cannot create Roller for $T because it has more than 22 parameters.")
    if (params.length == 0)
      c.abort(c.enclosingPosition, s"Cannot create Roller for $T because it has no parameters.")

    val tupleName = newTypeName(s"Tuple${params.length}")
    val types = params.map { param => tq"_root_.scala.Option[${param.returnType}]" }

    // params.head is safe because the case class has at least one member
    val firstFor = fq"""${params.head.name.asInstanceOf[c.TermName]} <- _root_.scala.Seq(_root_.scala.Some(in.${params.head}), _root_.scala.None)"""
    val restOfFors = params.tail.zip(params).map {
      case (param, prevParam) =>
        fq"""${param.name.asInstanceOf[c.TermName]} <- if (${prevParam.name.asInstanceOf[c.TermName]}.isDefined) _root_.scala.Seq(_root_.scala.Some(in.${param}), _root_.scala.None) else _root_.scala.Seq(_root_.scala.None)"""
    }
    val fors = firstFor :: restOfFors

    val names = params.map { param => param.name.asInstanceOf[c.TermName] }

    val cuber = q"""
    new _root_.com.twitter.algebird.macros.Roller[${T}] {
      type K = $tupleName[..$types]
      def apply(in: ${T}): _root_.scala.Seq[K] = for (..$fors) yield new K(..$names)
    }
    """
    c.Expr[Roller[T]](cuber)
  }
}
