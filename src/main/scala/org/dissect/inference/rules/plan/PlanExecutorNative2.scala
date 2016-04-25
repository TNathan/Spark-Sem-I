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

/**
  * An executor that works on the the native Scala data structures and uses Spark joins, filters etc.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorNative2(sc: SparkContext) extends PlanExecutor[RDD[RDFTriple], RDFGraphNative]{

  val sqlContext = new SQLContext(sc)
  val emptyGraph = EmptyRDFGraphDataFrame.get(sqlContext)

  def execute(plan: Plan, graph: RDFGraphNative): RDFGraphNative = {
    val logicalPlan = plan.toLogicalPlan(sqlContext)

    println(logicalPlan.toString())

//    val result = executePlan(logicalPlan, graph.toRDD())
//
//    new RDFGraphNative(graph.toRDD())
    graph
  }

//  def extract[T <: Product](tuple: T, positions: Seq[Int]): Product = {
//    val list = tuple.productIterator.toList
//    val newList = positions.map(pos => list(pos)).toTuple
//    newList
//  }
//
//  def genMapper[A, B](f: A => B): A => B = {
//    val locker = com.twitter.chill.MeatLocker(f)
//    x => locker.get.apply(x)
//  }
//
//  def executePlan[T <: Product](logicalPlan: LogicalPlan, triples: RDD[RDFTriple]): RDD[SolutionMapping] = {
//    logicalPlan match {
//      case logical.Join(left, right, Inner, Some(condition)) =>
//        println("JOIN")
//        val leftRDD = executePlan(left, triples)
//        val rightRDD = executePlan(right, triples)
//        leftRDD
//      case logical.Project(projectList, child) =>
//        println("PROJECT")
//        println(projectList.map(expr => expr.qualifiedName).mkString("--"))
//        val childExpressions = expressionsFor(child)
//
//        var rdd = executePlan(child, triples)
//
//        if(projectList.size < childExpressions.size) {
//          val positions = projectList.map(expr => childExpressions.indexOf(expr))
//
//          val r = executePlan(child, triples) map genMapper(tuple => extract(tuple, positions))
//          println(r)
//
//        }
//
//        rdd
//      case logical.Filter(condition, child) =>
//        println("FILTER")
//        val childRDD = executePlan(child, triples)
//        val childExpressions = expressionsFor(child)
//        applyFilter(condition, childExpressions, childRDD)
//      case default =>
//        triples.map(t => new SolutionMapping)
//    }
//  }
//
//  def applyFilter[T <: Product](condition: Expression, childExpressions: List[Expression], rdd: RDD[T]): RDD[T] = {
//    condition match {
//      case And(left: Expression, right: Expression) =>
//        applyFilter(right, childExpressions, applyFilter(left, childExpressions, rdd))
//      case EqualTo(left: Expression, right: Expression) =>
//        val value = right.toString()
//
//        val index = childExpressions.map(e => e.toString()).indexOf(left.toString())
//
//        rdd.filter(t => t.productElement(index) == value)
//      case _ => rdd
//    }
//  }
//
//  def applyFilter2(condition: Expression, childExpressions: List[Expression], rdd: RDD[_]): RDD[_] = {
//    condition match {
//      case And(left: Expression, right: Expression) =>
//        applyFilter2(right, childExpressions, applyFilter2(left, childExpressions, rdd))
//      case EqualTo(left: Expression, right: Expression) =>
//        val col = left.asInstanceOf[AttributeReference].name
//        val value = right.toString()
//
//        if(childExpressions.size == 3) {
//          var tmp = rdd.asInstanceOf[RDD[RDFTriple]]
//          tmp = if(col == "subject") {
//            tmp.filter{t => t.subject == value}
//          } else if(col == "predicate") {
//            tmp.filter{t => t.predicate == value}
//          } else {
//            tmp.filter{t => t.`object` == value}
//          }
//          tmp
//        } else {
//          rdd
//        }
//      case _ => rdd
//    }
//  }
//
//  def expressionsFor(logicalPlan: LogicalPlan): List[Expression] = {
//    logicalPlan match {
//      case logical.Join(left, right, Inner, Some(condition)) =>
//        expressionsFor(left) ++ expressionsFor(right)
//      case logical.Project(projectList, child) =>
//        projectList.toList
//      case logical.Filter(condition, child) =>
//        expressionsFor(condition)
//      case _ =>
//        logicalPlan.expressions.toList
//    }
//  }
//
//  def expressionsFor(expr: Expression): List[Expression] = {
//    expr match {
//      case And(left: Expression, right: Expression) =>
//        expressionsFor(left) ++ expressionsFor(right)
//      case EqualTo(left: Expression, right: Expression) =>
//        List(left)
//      case _ =>
//        Nil
//    }
//  }
//
//  def mergeJoins(joins: mutable.Set[Join]) = {
//    joins.groupBy(join => (join.tp1, join.tp2))
//  }
//
//  implicit class EnrichedWithToTuple[A](elements: Seq[A]) {
//    def toTuple: Product = elements.length match {
//      case 2 => toTuple2
//      case 3 => toTuple3
//      case 4 => toTuple2
//      case 5 => toTuple3
//    }
//    def toTuple2 = elements match {case Seq(a, b) => (a, b) }
//    def toTuple3 = elements match {case Seq(a, b, c) => (a, b, c) }
//    def toTuple4 = elements match {case Seq(a, b, c, d) => (a, b, c, d) }
//    def toTuple5 = elements match {case Seq(a, b, c, d, e) => (a, b, c, d, e) }
//
//  }
//
//  abstract class AbstractSolutionMapping[K, V](val var2Value: mutable.HashMap[K, V] = mutable.HashMap[K, V]()) {
//    def addMapping(variable: K, value: V): Unit = {
//      var2Value += variable -> value
//    }
//  }
//
//  class SolutionMapping extends AbstractSolutionMapping[String, String]
//    def this(triple: RDFTriple) = {
//      this(mutable.HashMap("S" -> "S"))
//    }
}
