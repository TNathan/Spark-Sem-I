package org.dissect.inference

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * @author Lorenz Buehmann
  *
  */
object Test {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    loadRDFGraph("dbpedia_sample.nt")
  }

  def loadRDFGraph (filename: String): Unit = {
    for (line <- Source.fromFile(filename).getLines) {
      println(line)
    }
  }

  /**
    * Get triples for the given property.
    *
    * @param triples the triples to be filtered
    * @param property the property
    * @return a set of triples that have the property as predicate
    */
  def getTriples(triples: Set[Tuple3], property: String): Set[Tuple3] = {
    triples.filter(_._2 == property)
  }

  /**
    * Rule:
    * uuu rdfs:subClassOf xxx .
    * vvv rdf:type uuu .
    * => vvv rdf:type xxx .
    *
    * @param triples
    * @return
    */
  def applyRDFS9(triples: Set[Tuple3], schemaTriples: Set[Tuple3]) : Set[Tuple3] = {
    var newTriples;

    triples.foreach(t => schemaTriples

    )
  }
}
