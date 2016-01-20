package org.dissect.inference.data

import org.slf4j.LoggerFactory

/**
  * Writes an RDF graph to disk.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphWriter {

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def writeToFile(graph: RDFGraph, path: String) = {
    logger.info("writing triples to disk...")
    val startTime  = System.currentTimeMillis()

    graph.triples
      .map(t => "<" + t._1 + "> <" + t._2 + "> <" + t._3 + "> .") // to N-TRIPLES string
//      .coalesce(8)
      .saveAsTextFile(path)

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis()-startTime) + "ms.")
  }
}
