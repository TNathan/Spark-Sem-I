package org.dissect.inference.rules.plan

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dissect.inference.data.{RDFGraph, RDFTriple}
import org.dissect.inference.rules.RDDOperations
import org.dissect.inference.utils.TripleUtils

import scala.collection.mutable

/**
  * An executor that works on the the native Scala data structures and uses Spark joins, filters etc.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorNative(sc: SparkContext) {

  def execute(plan: Plan, graph: RDFGraph): RDD[RDFTriple] = {
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

    graph.triples
  }

  def mergeJoins(joins: mutable.Set[Join]) = {
    joins.groupBy(join => (join.tp1, join.tp2))
  }
}
