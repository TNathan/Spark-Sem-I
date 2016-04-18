package org.dissect.inference.rules

import org.apache.jena.vocabulary.{OWL2, RDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFGraphLoader, RDFGraphWriter, RDFTriple}
import org.dissect.inference.rules.Planner.Plan
import org.dissect.inference.utils.{RuleUtils, TripleUtils}

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
    val p = ns + "p"
    triples += RDFTriple(p, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)

    for(i <- 1 to 10) {
      triples += RDFTriple(ns + "x" + i, p, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p, ns + "z" + i)
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

    val planExecutor = new PlanExecutorNative(sc)
    planExecutor.execute(plan, graph)

    sc.stop()
  }

  class PlanExecutorNative(sc: SparkContext) {
    def execute(plan: Plan, graph: RDFGraph) = {

      // for each triple pattern compute the relation first
      val relations = new mutable.HashMap[org.apache.jena.graph.Triple, RDD[RDFTriple]]()
      plan.triplePatterns.foreach{tp =>
//        println(tp)
        val rel = graph.find(tp)
//        println("REL\n" + rel.collect().mkString("\n"))
        relations += (tp -> rel)
      }

      // TODO order joins by dependencies
      plan.joins.foreach{join =>
        println(join)

        val rel1 = relations(join.tp1)
        val rel2 = relations(join.tp2)

        val joinVar = join.joinVar

        // convert RDD of relation 1 by position of join variable
        val tmp1 = TripleUtils.position(joinVar, join.tp1) match {
          case 1  => RDDOperations.subjKeyPredObj(rel1)
          case 2  => RDDOperations.predKeySubjObj(rel1)
          case 3  => RDDOperations.objKeySubjPred(rel1)
        }
        println("TMP1\n" + tmp1.collect().mkString("\n"))

        // convert RDD of relation 1 by position of join variable
        val tmp2 = TripleUtils.position(joinVar, join.tp2) match {
          case 1  => RDDOperations.subjKeyPredObj(rel2)
          case 2  => RDDOperations.predKeySubjObj(rel2)
          case 3  => RDDOperations.objKeySubjPred(rel2)
        }
        println("TMP2\n" + tmp2.collect().mkString("\n"))

        // perform join
        val res = tmp1.join(tmp2)
        println("RES\n" + res.collect().mkString("\n"))

      }
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

      val rel3 = RDDOperations.subjKeyPredObj(rel2) // (?y, (?p, ?z))
        .join(
        RDDOperations.objKeySubjPred(rel2)) // (?y, (?x, ?p))
      // -> (?y, ((?p, ?z),(?x, ?p))
      println("REL3\n" + rel3.collect().mkString("\n"))
      val result = rel3.map(e => RDFTriple(e._2._2._1, e._2._2._2, e._2._1._2))

      new RDFGraph(result)
    }
  }
}
