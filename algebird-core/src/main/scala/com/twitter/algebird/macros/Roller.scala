package com.twitter.algebird.macros

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

/**
 * Given a TupleN, produces a sequence of (N + 1) tuples each of arity N
 * such that, for all k from 0 to N, there is a tuple with k Somes
 * followed by (N - k) Nones.
 *
 * This is useful for comparing some metric across multiple layers of
 * some hierarchy.
 * For example, suppose we have some climate data represented as
 * case class Data(continent: String, country: String, city: String, temperature: Double)
 * and we want to know the average temperatures of
 *   - each continent
 *   - each (continent, country) pair
 *   - each (continent, country, city) triple
 *
 * Here we desire the (continent, country) and (continent, country, city)
 * pair because, for example, if we grouped by city instead of by
 * (continent, country, city), we would accidentally combine the results for
 * Paris, Texas and Paris, France.
 *
 * Then we could do
 * > import com.twitter.algebird.macros.Roller.roller
 * > val data: List[Data]
 * > val averageTemps: Map[(Option[String], Option[String], Option[String]), Double] =
 * > data.flatMap { d => roller((d.continent, d.country, d.city)).map((_, d)) }
 * >   .groupBy(_._1)
 * >   .mapValues { xs => val temps = xs.map(_.temperature); temps.sum / temps.length }
 */
trait Roller[I] {
  type K
  def apply(in: I): TraversableOnce[K]
}

object Roller {
  implicit def roller[T]: Roller[T] = macro rollerImpl[T]

  def rollerImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Roller[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val params = getParams(c)
    val arity = params.length
    if (arity > 22)
      c.abort(c.enclosingPosition, s"Cannot create Roller for $T because it has more than 22 parameters.")
    if (arity == 0)
      c.abort(c.enclosingPosition, s"Cannot create Roller for $T because it has no parameters.")

    val tupleName = {
      val types = getParamTypes(c)
      val optionTypes = types.map { t => tq"_root_.scala.Option[$t]" }
      val tupleType = newTypeName(s"Tuple${arity}")
      tq"_root_.scala.$tupleType[..$optionTypes]"
    }

    val somes = params.zip(Stream.from(1)).map {
      case (param, index) =>
        val name = newTermName(s"some$index")
        q"val $name = _root_.scala.Some(in.$param)"
    }

    val items = (0 to arity).map { i =>
      val args = (1 to arity).map { index =>
        val some = newTermName(s"some$index")
        if (index <= i) q"$some" else q"_root_.scala.None"
      }
      q"new K(..$args)"
    }

    val roller = q"""
    new _root_.com.twitter.algebird.macros.Roller[${T}] {
      type K = $tupleName
      def apply(in: ${T}): _root_.scala.Seq[K] = {
        ..$somes
        Seq(..$items)
      }
    }
    """
    c.Expr[Roller[T]](roller)
  }
}
