package org.dissect.inference.rules.plan

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, PrettyAttribute}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.dissect.inference.data.{EmptyRDFGraphDataFrame, RDFGraph, RDFTriple}
import org.dissect.inference.rules.RDDOperations
import org.dissect.inference.utils.TripleUtils

import scala.collection.mutable
import org.apache.spark.sql.catalyst.plans
import org.apache.spark.sql.catalyst.plans.{Inner, logical}
import org.apache.spark.sql.execution.LogicalRDD

/**
  * An executor that works on the the native Scala data structures and uses Spark joins, filters etc.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorNative(sc: SparkContext) {

  val sqlContext = new SQLContext(sc)
  val emptyGraph = EmptyRDFGraphDataFrame.get(sqlContext)

  def execute(plan: Plan, graph: RDFGraph): RDFGraph = {
    val logicalPlan = plan.toLogicalPlan(sqlContext)

    println(logicalPlan.toString())

    execute(logicalPlan, graph.triples)

    graph
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

  def execute(logicalPlan: LogicalPlan, triples: RDD[RDFTriple]): RDD[_] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition)) =>
        println("JOIN")
        val leftRDD = execute(left, triples)
        val rightRDD = execute(right, triples)
        leftRDD
      case logical.Project(projectList, child) =>
        println("PROJECT")
        println(projectList.map(expr => expr.qualifiedName).mkString("--"))
        val childExpressions = expressionsFor(child)

        var rdd = execute(child, triples)

//        if(projectList.size == 1) {
//          rdd =
//        }

        rdd
      case logical.Filter(condition, child) =>
        println("FILTER")
        var rdd = execute(child, triples)
        condition match {
          case EqualTo(left: Expression, right: Expression) =>
            val col = left.asInstanceOf[AttributeReference].name
            val value = right.toString()

            if(expressionsFor(child).size == 3) {
              val tmp = rdd.asInstanceOf[RDD[RDFTriple]]
              rdd = if(col == "subject") {
                tmp.filter{t => t.subject == value}
              } else if(col == "predicate") {
                tmp.filter{t => t.predicate == value}
              } else {
                tmp.filter{t => t.`object` == value}
              }
              rdd = tmp
            }
        }

        rdd
      case default =>
        println(default.getClass)
        triples
    }
  }

  def expressionsFor(logicalPlan: LogicalPlan): List[Expression] = {
    logicalPlan match {
      case logical.Join(left, right, Inner, Some(condition)) =>
        expressionsFor(left) ++ expressionsFor(right)
      case logical.Project(projectList, child) =>
        projectList.toList
      case logical.Filter(condition, child) =>
        condition match {
//          case And(left: Expression, right: Expression) =>
//            expressionsFor(left) ++ expressionsFor(right)
          case EqualTo(left: Expression, right: Expression) =>
            List(left)
        }
      case _ =>
        logicalPlan.expressions.toList
    }
  }

  case class Result[T <: Any](expressions: Seq[Expression], rdd: RDD[_]) {

  }

  def execute2(plan: Plan, graph: RDFGraph): RDFGraph = {
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
}
