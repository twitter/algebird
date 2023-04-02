

package com.twitter.algebird.macros

import com.twitter.algebird.*

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type
import scala.runtime.Tuples
object MonoidMacro:
  
  def summonAll[T: Type](using q: Quotes): List[Expr[Monoid[?]]] =
      Type.of[T] match
         case '[tpe *: tpes]   =>
            Expr.summon[Monoid[tpe]] match
              case None => derivedMonoid[tpe] :: summonAll[tpes]
              case Some(inst) => inst  :: summonAll[tpes]
         case '[EmptyTuple]     => Nil

  def derivedMonoid[T: Type](using q: Quotes): Expr[Monoid[T]] =
    import q.reflect.*
    val ev: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]] match
      case None =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to find or derive Monoid instance for ${tname}")
      case Some(expr) => expr

    

    ev match
      case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
        
        val insts = summonAll[elementTypes]
        val instsAsExpr = Expr.ofTupleFromSeq(insts)

        '{ 
            new Monoid[T]:
              override def zero: T =
                val t = applyZeros(${instsAsExpr}).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def plus(x:T,y:T):T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = SemigroupMacro.applyPlus(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
        
        }

      case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
        ???
                    
  private[macros] def applyZeros(a:Tuple):Tuple =
    a match
        case EmptyTuple => EmptyTuple
        case inst *: ts =>
            inst.asInstanceOf[Monoid[Any]].zero *: applyZeros(ts)


