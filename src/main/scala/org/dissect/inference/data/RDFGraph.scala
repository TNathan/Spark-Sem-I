package org.dissect.inference.data

import org.apache.spark.rdd.RDD

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
case class RDFGraph (triples: RDD[RDFTriple]) {

}
