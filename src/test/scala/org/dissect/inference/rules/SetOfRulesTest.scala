package org.dissect.inference.rules

import org.apache.jena.vocabulary.{OWL2, RDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFGraphWriter, RDFTriple}
import org.dissect.inference.forwardchaining.ForwardRuleReasonerNaive
import org.dissect.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @author Lorenz Buehmann
  */
object SetOfRulesTest {

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

    val scale = 1

    var begin = 1
    var end = 10 * scale
    for(i <- begin to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
    }

    begin = end + 1
    end = begin + 10 * scale
    for(i <- begin to end) { // should not produce (?x_i, p1, ?z_i) as p1 and p2 are used
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p2, ns + "z" + i)
    }

    begin = end + 1
    end = begin + 10 * scale
    for(i <- begin to end) { // should not produce (?x_i, p3, ?z_i) as p3 is not transitive
      triples += RDFTriple(ns + "x" + i, p3, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p3, ns + "z" + i)
    }

    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    val graph = new RDFGraph(triplesRDD)

    val rules = RuleUtils.load("rdfs-simple.rules")

    val reasoner = new ForwardRuleReasonerNaive(sc, rules.toSet)

    val res = reasoner.apply(graph)

    RDFGraphWriter.writeToFile(res, "/tmp/spark-tests/naive")

    sc.stop()
  }
}
