package com.twitter.algebird.macros

import scala.language.experimental.{macros => sMacros}
import com.twitter.algebird.macros.MacroCompat._

/**
 * "Cubes" a case class or tuple, i.e. for a tuple of type (T1, T2, ... , TN) generates all 2^N possible
 * combinations of type (Option[T1], Option[T2], ... , Option[TN]).
 *
 * This is useful for comparing some metric across all possible subsets. For example, suppose we have a set of
 * people represented as case class Person(gender: String, age: Int, height: Double) and we want to know the
 * average height of
 *   - people, grouped by gender and age
 *   - people, grouped by only gender
 *   - people, grouped by only age
 *   - all people
 *
 * Then we could do > import com.twitter.algebird.macros.Cuber.cuber > val people: List[People] > val
 * averageHeights: Map[(Option[String], Option[Int]), Double] = > people.flatMap { p => cuber((p.gender,
 * p.age)).map((_,p)) } > .groupBy(_._1) > .mapValues { xs => val heights = xs.map(_.height); heights.sum /
 * heights.length }
 */
trait Cuber[I] {
  type K
  def apply(in: I): TraversableOnce[K]
}

object Cuber {
  implicit def cuber[T]: Cuber[T] = macro cuberImpl[T]

  def cuberImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Cuber[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val params = getParams(c)
    val arity = params.length
    if (arity > 22)
      c.abort(c.enclosingPosition, s"Cannot create Cuber for $T because it has more than 22 parameters.")
    if (arity == 0)
      c.abort(c.enclosingPosition, s"Cannot create Cuber for $T because it has no parameters.")

    val tupleName = {
      val types = getParamTypes(c)
      val optionTypes = types.map(t => tq"_root_.scala.Option[$t]")
      val tupleType = typeName(c)(s"Tuple$arity")
      tq"_root_.scala.$tupleType[..$optionTypes]"
    }

    val somes = params.zip(Stream.from(1)).map { case (param, index) =>
      val name = termName(c)(s"some$index")
      q"val $name = _root_.scala.Some(in.$param)"
    }

    val options = (1 to arity).map { index =>
      val some = termName(c)(s"some$index")
      q"if (((1 << ${index - 1}) & i) == 0) _root_.scala.None else $some"
    }

    val cuber = q"""
    new _root_.com.twitter.algebird.macros.Cuber[$T] {
      type K = $tupleName
      def apply(in: $T): _root_.scala.Seq[K] = {
        ..$somes
        (0 until (1 << $arity)).map { i =>
          new K(..$options)
        }
      }
    }
    """
    c.Expr[Cuber[T]](cuber)
  }
}
