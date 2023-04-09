package com.twitter.algebird.macros

import com.twitter.algebird.*

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type
import scala.runtime.Tuples
import org.scalacheck.{Arbitrary, Gen}

object ArbitraryCaseClassMacro:
  inline def gen[T]: Gen[T] = ${ caseClassGen[T] }
  inline def arbitrary[T]: Arbitrary[T] = ${ caseClassArbitrary[T] }

  def summonAll[T: Type](using q: Quotes): List[Expr[Gen[?]]] =
    Type.of[T] match
      case '[tpe *: tpes] =>
        Expr.summon[Arbitrary[tpe]] match
          case None       => caseClassGen[tpe] :: summonAll[tpes]
          case Some(inst) => '{ ${ inst }.arbitrary } :: summonAll[tpes]
      case '[EmptyTuple] => Nil
  def caseClassArbitrary[T: Type](using q: Quotes): Expr[Arbitrary[T]] =
    import q.reflect.*
    val gen = caseClassGen[T]
    '{
      Arbitrary[T]($gen)
    }

  def caseClassGen[T: Type](using q: Quotes): Expr[Gen[T]] =
    import q.reflect.*
    val ev: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]] match
      case None =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(
          s"unable to find or derive Arbitrary instance for ${tname}"
        )
      case Some(expr) => expr

    ev match
      case '{
            $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes }
          } =>
        val insts: List[Expr[Gen[?]]] = summonAll[elementTypes].reverse
        val last :: leading = insts: @unchecked

        val expr =
          leading.foldLeft[Seq[Expr[?]] => Expr[Gen[T]]] { args =>
            val argsExpr = Expr.ofSeq(args)
            '{
              ${ last }.map { v =>
                ${ m }.fromTuple(
                  Tuple
                    .fromArray((${ argsExpr } :+ v).toArray)
                    .asInstanceOf[elementTypes]
                )
              }
            }
          } { case (acc, gen) =>
            args =>
              '{
                ${ gen }.flatMap { v =>
                  ${ acc(args :+ '{ v }) }
                }
              }
          }
        expr(Nil)

      case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to derive Arbitrary for ${tname}")
