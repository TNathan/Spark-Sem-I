package org.dissect.inference.data

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/**
  * Loads an RDF graph from disk or a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphLoader {

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def loadFromFile(path: String, sc: SparkContext, minPartitions: Int = 2): RDFGraph = {
    logger.info("loading triples from disk...")
    val startTime  = System.currentTimeMillis()

    val triples =
      sc.textFile(path, minPartitions)
        .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
        .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    logger.info("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis()-startTime) + "ms.")
    new RDFGraph(triples)
  }
}
