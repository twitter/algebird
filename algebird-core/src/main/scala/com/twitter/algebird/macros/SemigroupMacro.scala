package com.twitter.algebird.macros

import com.twitter.algebird._
import com.twitter.algebird.macros.MacroCompat._

object SemigroupMacro {
  def caseClassSemigroup[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Semigroup[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val implicitSemigroups = getParams(c).map { param =>
      q"implicitly[_root_.com.twitter.algebird.Semigroup[${param.typeSignatureIn(T.tpe)}]]"
    }

    val res = q"""
    new _root_.com.twitter.algebird.Semigroup[$T] {
      ${plus(c)(implicitSemigroups)}
      ${sumOption(c)(implicitSemigroups)}
    }
    """
    c.Expr[Semigroup[T]](res)
  }

  def plus[T](c: Context)(implicitInstances: List[c.Tree])(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = getCompanionObject(c)
    val plusList = getParams(c).zip(implicitInstances).map { case (param, instance) =>
      q"$instance.plus(l.$param, r.$param)"
    }

    q"def plus(l: $T, r: $T): $T = $companion.apply(..$plusList)"
  }

  def sumOption[T](c: Context)(implicitInstances: List[c.Tree])(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val params = getParams(c)
    val companion = getCompanionObject(c)

    val sumOptionsGetted: List[c.Tree] =
      params.map(param => q"${param.name.asInstanceOf[TermName]}.get")
    val getSumOptions = params.zip(implicitInstances).map { case (param, instance) =>
      q"val ${param.name.asInstanceOf[TermName]} = $instance.sumOption(items.iterator.map(_.$param))"
    }

    val result = q"$companion.apply(..$sumOptionsGetted)"

    val getBlock = Block(getSumOptions, result)

    q"""
    override def sumOption(to: _root_.scala.collection.TraversableOnce[$T]): _root_.scala.Option[$T] =
      if (to.isEmpty) None
      else {
        val buf = new _root_.com.twitter.algebird.ArrayBufferedOperation[$T,$T](1000) with _root_.com.twitter.algebird.BufferedReduce[$T] {
          def operate(items: _root_.scala.Seq[$T]): $T = {
            $getBlock
          }
        }
        to.foreach(buf.put(_))
        buf.flush
      }
    """
  }

}
