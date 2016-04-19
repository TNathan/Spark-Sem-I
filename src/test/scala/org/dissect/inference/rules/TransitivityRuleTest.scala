package org.dissect.inference.rules

import org.apache.jena.vocabulary.{OWL2, RDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFGraphLoader, RDFGraphWriter, RDFTriple}
import org.dissect.inference.rules.plan.{Join, Plan}
import org.dissect.inference.utils.{RuleUtils, TripleUtils}

import scala.math.Ordering.Implicits._
import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @author Lorenz Buehmann
  */
object TransitivityRuleTest {

  def main(args: Array[String]) {
    // the SPARK config
    val conf = new SparkConf().setAppName("SPARK Reasoning")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.setMaster("local[2]")
    conf.set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)

    // generate graph
    val triples = new mutable.HashSet[RDFTriple]()
    val ns = "http://ex.org/"
    val p1 = ns + "p1"
    val p2 = ns + "p2"
    val p3 = ns + "p3"
    triples += RDFTriple(p1, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)
    triples += RDFTriple(p2, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)

    for(i <- 1 to 10) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
    }

    for(i <- 11 to 20) { // should not produce (?x_i, p1, ?z_i) as p1 and p2 are used
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p2, ns + "z" + i)
    }

    for(i <- 21 to 30) { // should not produce (?x_i, p3, ?z_i) as p3 is not transitive
      triples += RDFTriple(ns + "x" + i, p3, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p3, ns + "z" + i)
    }

    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    val graph = new RDFGraph(triplesRDD)

    // create reasoner
    val reasoner = new TransitivityRuleReasoner(sc)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, "/tmp/spark-tests")


    val rules = RuleUtils.load("test.rules")
    val rule = RuleUtils.byName(rules, "prp-trp").get
    val plan = Planner.rewrite(rule)

    val planExecutor1 = new PlanExecutorNative(sc)
    planExecutor1.execute(plan, graph)

    val planExecutor2 = new PlanExecutorSQL(sc)
    planExecutor2.execute(plan, graph)



    sc.stop()
  }

  class PlanExecutorNative(sc: SparkContext) {
    def execute(plan: Plan, graph: RDFGraph) = {
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

        println(plan.toSQL(tp1))

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
    }

    def mergeJoins(joins: mutable.Set[Join]) = {
      joins.groupBy(join => (join.tp1, join.tp2))
    }
  }

  class PlanExecutorSQL(sc: SparkContext) {
    def execute(plan: Plan, graph: RDFGraph) = {
      println(plan.toSQL)
    }
  }

  class TransitivityRuleReasoner(sc: SparkContext) {

    def apply(graph: RDFGraph) = {
      val startTime = System.currentTimeMillis()

      val triplesRDD = graph.triples.cache()

      //  [ prp-trp: (?p rdf:type owl:TransitiveProperty) (?x ?p ?y) (?y ?p ?z) -> (?x ?p ?z) ]
      val rel1 = triplesRDD
        .filter(t =>
          t.predicate == RDF.`type`.getURI &&
            t.`object` == OWL2.TransitiveProperty.getURI)
        .map(t => (t.subject, Nil)) // -> (?p, Nil)
      println("REL1\n" + rel1.collect().mkString("\n"))

      val rel2 = triplesRDD
        .map(t => (t.predicate, (t.subject, t.`object`))) // (?p, (?x, ?y))
        .join(rel1) // (?p, ((?x, ?y), Nil))
        .map(e => RDFTriple(e._2._1._1, e._1, e._2._1._2)) // (?x, ?p, ?y)
      println("REL2\n" + rel2.collect().mkString("\n"))

      val rel3 = RDDOperations.subjPredKeyObj(rel2) // ((?y, ?p), ?z)
        .join(
        RDDOperations.objPredKeySubj(rel2)) // ((?y, ?p), ?x)
      // -> ((?y, ?p) , (?z, ?x))
      println("REL3\n" + rel3.collect().mkString("\n"))
      val result = rel3.map(e => RDFTriple(e._2._2, e._1._2, e._2._1))

      new RDFGraph(result)
    }
  }
}
