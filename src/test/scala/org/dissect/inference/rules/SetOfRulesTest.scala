package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data._
import org.dissect.inference.forwardchaining.{ForwardRuleReasonerNaive, ForwardRuleReasonerOptimizedNative, ForwardRuleReasonerOptimizedSQL}
import org.dissect.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @author Lorenz Buehmann
  */
object SetOfRulesTest {

  val sparkSession = SparkSession.builder
    .master("local[4]")
    .appName("SPARK Reasoning")
    .config("spark.eventLog.enabled", "true")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  def main(args: Array[String]) {
    // generate graph
    val scale = 100000
    val triples = generateData(scale)

    // make RDD
    println("distributing...")
    val triplesRDD = sc.parallelize(triples.toSeq, 4)
    triplesRDD.toDebugString

    // create graph
    val graph = new RDFGraphNative(triplesRDD)

    val ruleNames = Set(
      "rdfs7",
      "prp-trp",
      "rdfs9"
    )

    val numberOfTriples = graph.size()
    println("#Triples:" + numberOfTriples)

    val rules = RuleUtils.load("rdfs-simple.rules")//.filter(r => ruleNames.contains(r.getName))

//    runNaive(graph, rules)
//    runNative(graph, rules)
    runSQL(graph, rules)

    sc.stop()
  }

  def generateData(scale: Integer) = {
    println("generating data...")
    val triples = new mutable.HashSet[RDFTriple]()
    val ns = "http://ex.org/"
    val p1 = ns + "p1"
    val p2 = ns + "p2"
    val p3 = ns + "p3"
    val c1 = ns + "C1"
    val c2 = ns + "C2"
    triples += RDFTriple(p1, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)
    triples += RDFTriple(p2, RDF.`type`.getURI, OWL2.TransitiveProperty.getURI)
    triples += RDFTriple(p1, RDFS.subPropertyOf.getURI, p2)
    triples += RDFTriple(c1, RDFS.subClassOf.getURI, c2)

    var begin = 1
    var end = 10 * scale
    for (i <- begin to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
    }

    begin = end + 1
    end = begin + 10 * scale
    for (i <- begin to end) {
      // should not produce (?x_i, p1, ?z_i) as p1 and p2 are used
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p2, ns + "z" + i)
    }

    begin = end + 1
    end = begin + 10 * scale
    for (i <- begin to end) {
      // should not produce (?x_i, p3, ?z_i) as p3 is not transitive
      triples += RDFTriple(ns + "x" + i, p3, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p3, ns + "z" + i)
    }

    // C1(c_i)
    begin = 1
    end = 10 * scale
    for (i <- begin to end) {
      triples += RDFTriple(ns + "c" + i, RDF.`type`.getURI, c1)
    }

    triples
  }

  def runNaive(graph: RDFGraphNative, rules: Seq[Rule]) = {
    val reasoner = new ForwardRuleReasonerNaive(sc, rules.toSet)
    val res = reasoner.apply(graph)
    RDFGraphWriter.writeToFile(res.toRDD(), "/tmp/spark-tests/naive")
  }

  def runNative(graph: RDFGraphNative, rules: Seq[Rule]) = {
    val reasoner = new ForwardRuleReasonerOptimizedNative(sc, rules.toSet)
    val res = reasoner.apply(graph)
    RDFGraphWriter.writeToFile(res.toRDD(), "/tmp/spark-tests/optimized-native")
  }

  def runSQL(graph: RDFGraphNative, rules: Seq[Rule]) = {
    // create Dataframe based graph
    val graphDataframe = new RDFGraphDataFrame(graph.toDataFrame(sparkSession)).cache()

    val reasoner = new ForwardRuleReasonerOptimizedSQL(sparkSession, rules.toSet)
    val res = reasoner.apply(graphDataframe)
    RDFGraphWriter.writeToFile(res.toDataFrame(), "/tmp/spark-tests/optimized-sql")
    reasoner.showExecutionStats()
  }
}
