package org.dissect.inference.rules.plan

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.{Inner, logical}
import org.dissect.inference.data._
import org.dissect.inference.rules.RDDOperations
import org.dissect.inference.utils.TripleUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * An executor that works on the the native Scala data structures and uses Spark joins, filters etc.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorNative(sc: SparkContext) extends PlanExecutor[RDD[RDFTriple], RDFGraphNative]{

  val sqlContext = new SQLContext(sc)
  val emptyGraph = EmptyRDFGraphDataFrame.get(sqlContext)

  def execute(plan: Plan, graph: RDFGraphNative): RDFGraphNative = {
    val logicalPlan = plan.toLogicalPlan(sqlContext)

    println(logicalPlan.toString())

    val result = executePlan(logicalPlan, graph.toRDD().asInstanceOf[RDD[Product]])

    new RDFGraphNative(graph.toRDD())
  }

//  def execute(logicalPlan: LogicalPlan, triples: RDD[RDFTriple]): Result = {
//      logicalPlan match {
//        case logical.Join(left, right, Inner, Some(condition)) =>
//          println("JOIN")
//          val leftRDD = execute(left, triples)
//          val rightRDD = execute(right, triples)
//          leftRDD
//        case logical.Project(projectList, child) =>
//          println("PROJECT")
//          println(projectList.map(expr => expr.qualifiedName).mkString("--"))
//          val rdd = execute(child, triples)
//          rdd
//        case logical.Filter(condition, child) =>
//          println("FILTER")
//          var res = execute(child, triples)
//          condition match {
//            case EqualTo(left: Expression, right: Expression) =>
//              val col = left.asInstanceOf[AttributeReference].name
//              val value = right.toString()
//
//              if(res.expressions.size == 3) {
//                var rdd = res.rdd.asInstanceOf[RDD[RDFTriple]]
//                rdd = if(col == "subject") {
//                  rdd.filter{t => t.subject == value}
//                } else if(col == "predicate") {
//                  rdd.filter{t => t.predicate == value}
//                } else {
//                  rdd.filter{t => t.`object` == value}
//                }
//                res = Result(res.expressions, rdd)
//              }
//
//          }
//
//          rdd
//        case _ =>
//          triples
//      }
//  }

  def extract[T <: Product](tuple: T, positions: Seq[Int]): Product = {
    val list = tuple.productIterator.toList
    val newList = positions.map(pos => list(pos)).toTuple
    newList
  }

  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }

  def asKeyValue(tuple: Product, keyPositions: Seq[Int]): (Product, Product) = {
    val key = keyPositions.map(pos => tuple.productElement(pos)).toTuple
    val value = for (i <- 0 to tuple.productArity; if !keyPositions.contains(i)) yield tuple.productElement(i)

    (key -> value.toTuple)
  }

  def toPairRDD[T >: Product](tuples: RDD[Product], joinPositions: Seq[Int]): RDD[(Product, Product)] = {
    tuples map genMapper(t => asKeyValue(t, joinPositions))
  }

  def mergeKeyValue(pair: (Product, Product), joinPositions: Seq[Int]): Product = {
    val list = new ListBuffer[Any]
    for(i <- 0 to pair._1.productArity) {
      list.insert(joinPositions(i), pair._1.productElement(i))
    }

    for(i <- 0 to pair._2.productArity) {
      list += pair._2.productElement(i)
    }

    val it = pair._1.productIterator ++ pair._2.productIterator

    list.toTuple
  }

  def mergedRDD(tuples: RDD[(Product, Product)], joinPositions: Seq[Int]) = RDD[Product] = {
    tuples map genMapper(t => mergeKeyValue(t, joinPositions))
  }

  def executePlan[T >: Product, U <: Product](logicalPlan: LogicalPlan, triples: RDD[Product]): RDD[Product] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition)) =>
        println("JOIN")
        val leftRDD = executePlan(left, triples)
        val rightRDD = executePlan(right, triples)

        val joinExpressions = expressionsFor(condition)

        val leftExpressions = expressionsFor(left)
        val rightExpressions = expressionsFor(right)

        val joinExpressionsLeft = leftExpressions.intersect(joinExpressions)
        val joinExpressionsRight = rightExpressions.intersect(joinExpressions)

        val joinPositionsLeft = joinExpressionsLeft.map(expr => leftExpressions.indexOf(expr))
        val joinPositionsRight = joinExpressionsRight.map(expr => rightExpressions.indexOf(expr))

        val l = toPairRDD(leftRDD, joinPositionsLeft)
        val r = toPairRDD(rightRDD, joinPositionsRight)

        val joinedRDD = l.join(r)

        // map it back to tuples

        val merged = mergedRDD(joinedRDD, joinPositionsLeft)
        leftRDD
      case logical.Project(projectList, child) =>
        println("PROJECT")
        println(projectList.map(expr => expr.qualifiedName).mkString("--"))
        val childExpressions = expressionsFor(child)

        var rdd = executePlan(child, triples)

        if(projectList.size < childExpressions.size) {
          val positions = projectList.map(expr => childExpressions.indexOf(expr))

          rdd = rdd map genMapper(tuple => extract(tuple, positions))
        }

        rdd
      case logical.Filter(condition, child) =>
        println("FILTER")
        val childRDD = executePlan(child, triples)
        val childExpressions = expressionsFor(child)
        applyFilter(condition, childExpressions, childRDD)
      case default =>
        println(default.simpleString)
        triples
    }
  }

  def executePlan2[T <: Product](logicalPlan: LogicalPlan, triples: RDD[T]): RDD[T] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition)) =>
        println("JOIN")
        val leftRDD = executePlan2(left, triples)
        val rightRDD = executePlan2(right, triples)
        leftRDD
      case logical.Project(projectList, child) =>
        println("PROJECT")
        println(projectList.map(expr => expr.qualifiedName).mkString("--"))
        val childExpressions = expressionsFor(child)

        var rdd = executePlan2(child, triples)

        if(projectList.size < childExpressions.size) {
          val positions = projectList.map(expr => childExpressions.indexOf(expr))

          val r = executePlan2(child, triples) map genMapper(tuple => extract(tuple, positions))
          println(r)

        }

        rdd
      case logical.Filter(condition, child) =>
        println("FILTER")
        val childRDD = executePlan2(child, triples)
        val childExpressions = expressionsFor(child)
        applyFilter(condition, childExpressions, childRDD)
      case default =>
        println(default.getClass)
        triples
    }
  }

  def applyFilter[T <: Product](condition: Expression, childExpressions: List[Expression], rdd: RDD[T]): RDD[T] = {
    condition match {
      case And(left: Expression, right: Expression) =>
        applyFilter(right, childExpressions, applyFilter(left, childExpressions, rdd))
      case EqualTo(left: Expression, right: Expression) =>
        val value = right.toString()

        val index = childExpressions.map(e => e.toString()).indexOf(left.toString())

        rdd.filter(t => t.productElement(index) == value)
      case _ => rdd
    }
  }

  def applyFilter2(condition: Expression, childExpressions: List[Expression], rdd: RDD[_]): RDD[_] = {
    condition match {
      case And(left: Expression, right: Expression) =>
        applyFilter2(right, childExpressions, applyFilter2(left, childExpressions, rdd))
      case EqualTo(left: Expression, right: Expression) =>
        val col = left.asInstanceOf[AttributeReference].name
        val value = right.toString()

        if(childExpressions.size == 3) {
          var tmp = rdd.asInstanceOf[RDD[RDFTriple]]
          tmp = if(col == "subject") {
            tmp.filter{t => t.subject == value}
          } else if(col == "predicate") {
            tmp.filter{t => t.predicate == value}
          } else {
            tmp.filter{t => t.`object` == value}
          }
          tmp
        } else {
          rdd
        }
      case _ => rdd
    }
  }

  def expressionsFor(logicalPlan: LogicalPlan): List[Expression] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition)) =>
        expressionsFor(left) ++ expressionsFor(right)
      case logical.Project(projectList, child) =>
        projectList.toList
      case logical.Filter(condition, child) =>
        expressionsFor(condition)
      case _ =>
        logicalPlan.expressions.toList
    }
  }

  def expressionsFor(expr: Expression): List[Expression] = {
    expr match {
      case And(left: Expression, right: Expression) =>
        expressionsFor(left) ++ expressionsFor(right)
      case EqualTo(left: Expression, right: Expression) =>
        List(left)
      case _ =>
        Nil
    }
  }

  case class Result[T <: Any](expressions: Seq[Expression], rdd: RDD[_]) {

  }

  def execute2(plan: Plan, graph: RDFGraphNative): RDFGraphNative = {
    println("JOIN CANDIDATES:\n" + plan.joins.mkString("\n"))

    // for each triple pattern compute the relation first
    val relations = new mutable.HashMap[org.apache.jena.graph.Triple, RDD[RDFTriple]]()

    plan.triplePatterns.foreach{tp =>
      //        println(tp)
      val rel = graph.find(tp)
      println("REL\n" + rel.collect().mkString("\n"))
      relations += (tp -> rel)
    }

    // TODO order joins by dependencies
    val joins = plan.joins

    // we merge joins with the same triple patterns
    val mergedJoins = mergeJoins(joins)

    mergedJoins.foreach{entry =>

      val tp1 = entry._1._1
      val tp2 = entry._1._2

      val joinVars = entry._2.map(join => join.joinVar)

      println("JOIN: " + tp1 + " JOIN " + tp2 + " ON " + joinVars)

      val rel1 = relations(tp1)
      val rel2 = relations(tp2)

      val res =
        if (joinVars.size == 1) {
          // convert RDD of relation 1 by position of join variables
          val tmp1 =
            TripleUtils.position(joinVars.head, tp1) match {
              case 1 => RDDOperations.subjKeyPredObj(rel1)
              case 2 => RDDOperations.predKeySubjObj(rel1)
              case 3 => RDDOperations.objKeySubjPred(rel1)
            }
          println("TMP1\n" + tmp1.collect().mkString("\n"))

          // convert RDD of relation 2 by position of join variables
          val tmp2 =
            TripleUtils.position(joinVars.head, tp2) match {
              case 1 => RDDOperations.subjKeyPredObj(rel2)
              case 2 => RDDOperations.predKeySubjObj(rel2)
              case 3 => RDDOperations.objKeySubjPred(rel2)
            }
          println("TMP2\n" + tmp2.collect().mkString("\n"))

          // perform join
          tmp1.join(tmp2)
        } else {
          // convert RDD of relation 1 by position of join variables
          val positions1 = Seq(joinVars.map(v => TripleUtils.position(v, tp1)).toList).map(tup => tup(0) -> tup(1)).head
          val tmp1 =
            positions1 match {
              case (1, 2) => RDDOperations.subjPredKeyObj(rel2)
              case (1, 3) => RDDOperations.subjObjKeyPred(rel2)
              case (2, 3) => RDDOperations.objPredKeySubj(rel2)
            }
          println("TMP1\n" + tmp1.collect().mkString("\n"))

          // convert RDD of relation 2 by position of join variables
          val positions2 = Seq(joinVars.map(v => TripleUtils.position(v, tp2)).toList).map(tup => tup(0) -> tup(1)).head
          val tmp2 =
            positions2 match {
              case (1, 2) => RDDOperations.subjPredKeyObj(rel2)
              case (1, 3) => RDDOperations.subjObjKeyPred(rel2)
              case (2, 3) => RDDOperations.objPredKeySubj(rel2)
            }
          println("TMP2\n" + tmp2.collect().mkString("\n"))

          // perform join
          tmp1.join(tmp2)
        }
      println("RES\n" + res.collect().mkString("\n"))
    }

    graph
  }

  def mergeJoins(joins: mutable.Set[Join]) = {
    joins.groupBy(join => (join.tp1, join.tp2))
  }

  implicit class EnrichedWithToTuple[A](elements: Seq[A]) {
    def toTuple: Product = elements.length match {
      case 2 => toTuple2
      case 3 => toTuple3
      case 4 => toTuple4
      case 5 => toTuple5
    }
    def toTuple2 = elements match {case Seq(a, b) => (a, b) }
    def toTuple3 = elements match {case Seq(a, b, c) => (a, b, c) }
    def toTuple4 = elements match {case Seq(a, b, c, d) => (a, b, c, d) }
    def toTuple5 = elements match {case Seq(a, b, c, d, e) => (a, b, c, d, e) }

  }

  abstract class AbstractSolutionMapping[K, V]() {
    val var2Value = mutable.HashMap[K, V]()

    def addMapping(variable: K, value: V) = var2Value += variable -> value
  }

  class SolutionMapping extends AbstractSolutionMapping[String, String]

}
