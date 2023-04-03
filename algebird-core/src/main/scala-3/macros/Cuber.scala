package com.twitter.algebird.macros
import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type
import scala.runtime.Tuples

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
trait Cuber[I]:
  type K
  def apply(in: I): TraversableOnce[K]

object Cuber:
  inline def cuber[T]: Cuber[T] = ${deriveCuberImpl[T]}
  inline def derived[T]: Cuber[T] = ${deriveCuberImpl[T]}
  def deriveCuberImpl[T:Type](using q:Quotes):Expr[Cuber[T]] =
    import q.reflect.*
    val ev: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]] match
      case None =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to derive Cuber instance for ${tname}")
      case Some(expr) => expr

    
    ev match
      case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
        val caseclassFields = TypeRepr.of[T].typeSymbol.caseFields
        val arity = Expr(caseclassFields.length)
        val somes:List[Symbol] = caseclassFields.map(sym => Symbol.newVal(Symbol.spliceOwner,sym.name,TypeRepr.of[Option[Any]]
            ,Flags.EmptyFlags,Symbol.noSymbol))
        val idents = Expr.ofList(somes.map(sym => Ident(sym.termRef).asExpr))
        inline def somesStmt(caseclass:Expr[T]):List[ValDef] = somes.zip(caseclassFields).map((someSym,field) => ValDef(someSym,Some{  
            val value = Select(caseclass.asTerm,field).asExpr
            '{Some(${value})}.asTerm
            }
        ))
        
        def options(i:Expr[Int]):Expr[Seq[Option[Any]]] = 
            '{(1 to ${arity}).map(index =>
                if (((1 << {index - 1}) & ${i}) == 0) then
                    None
                else
                    ${idents}(index-1).asInstanceOf[Option[Any]]
            )
            }
        def blc(e:Expr[T]):Expr[Any] = Block(
            somesStmt(e),
            '{
                (0 until (1 << ${arity})).map{ i =>
                    Tuple.fromArray(${options('{i})}.toArray)
                }
            }.asTerm
        ).asExpr
        
        '{

            new Cuber[T]:
                type K = Tuple
                override def apply(in: T): IterableOnce[K] =
                    ${blc('{in})}.asInstanceOf[scala.collection.IterableOnce[K]]
        }

      case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to derive Cuber for ${tname}")

