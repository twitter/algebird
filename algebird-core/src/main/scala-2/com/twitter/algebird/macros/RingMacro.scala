package com.twitter.algebird.macros

import com.twitter.algebird._
import com.twitter.algebird.macros.MacroCompat._

object RingMacro {
  def caseClassRing[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Ring[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val params = getParams(c)
    val companion = getCompanionObject(c)

    val implicitRings = params.map { param =>
      q"implicitly[_root_.com.twitter.algebird.Ring[${param.typeSignatureIn(T.tpe)}]]"
    }

    val timesList = params.zip(implicitRings).map { case (param, instance) =>
      q"$instance.times(l.$param, r.$param)"
    }
    val oneList = implicitRings.map(instance => q"$instance.one")

    val res = q"""
    new _root_.com.twitter.algebird.Ring[$T] {
      ${SemigroupMacro.plus(c)(implicitRings)}
      ${SemigroupMacro.sumOption(c)(implicitRings)}
      ${MonoidMacro.zero(c)(implicitRings)}
      ${GroupMacro.negate(c)(implicitRings)}
      def times(l: $T, r: $T): $T = $companion.apply(..$timesList)
      def one: $T = $companion.apply(..$oneList)
    }
    """
    c.Expr[Ring[T]](res)
  }

}
