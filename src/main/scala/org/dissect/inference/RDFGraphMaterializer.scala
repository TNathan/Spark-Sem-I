package org.dissect.inference

import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFTriple, RDFGraph, RDFGraphWriter, RDFGraphLoader}
import org.dissect.inference.forwardchaining.{ForwardRuleReasonerNaive, ForwardRuleReasonerRDFS}
import org.dissect.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * The class to compute the RDFS materialization of a given RDF graph.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphMaterializer {


//  def main(args: Array[String]) {
//
//    if (args.length < 2) {
//      System.err.println("Usage: RDFGraphMaterializer <sourceFile> <targetFile>")
//      System.exit(1)
//    }
//
//    // the SPARK config
//    val conf = new SparkConf().setAppName("SPARK Reasoning")
//    conf.set("spark.hadoop.validateOutputSpecs", "false")
//    conf.setMaster("local[2]")
//    conf.set("spark.eventLog.enabled", "true")
//    val sc = new SparkContext(conf)
//
//    // load triples from disk
//    val graph = RDFGraphLoader.loadFromFile(args(0), sc, 2)
//
//    // create reasoner
//    val reasoner = new ForwardRuleReasonerRDFS(sc)
//
//    // compute inferred graph
//    val inferredGraph = reasoner.apply(graph)
//
//    // write triples to disk
//    RDFGraphWriter.writeToFile(inferredGraph, args(1))
//
//    sc.stop()
//  }

  def main(args: Array[String]) {
    // the SPARK config
    val conf = new SparkConf().setAppName("SPARK Reasoning")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.setMaster("local[2]")
//    conf.set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)

    val triples = new mutable.HashSet[RDFTriple]()
    triples.add(RDFTriple("s", "p", "o"))

    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    val graph = new RDFGraph(triplesRDD)

    val filenames = List(
      "rules/rdfs-simple.rules"
    )

    filenames.foreach { filename =>
      println(filename)

      // parse the rules
      val rules = RuleUtils.load(filename).toSet

      val reasoner = new ForwardRuleReasonerNaive(sc, rules)

      reasoner.apply(graph)
    }

  }
}
