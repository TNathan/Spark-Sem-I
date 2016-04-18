package org.dissect.inference.data

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
case class RDFGraph (triples: RDD[RDFTriple]) {

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  def find (s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): RDD[RDFTriple]= {
      triples.filter(t =>
          (s == None || t.subject == s) &&
          (p == None || t.predicate == p) &&
          (o == None || t.`object` == o)
      )
  }

  def find (triple: Triple): RDD[RDFTriple]= {
    find(
      if(triple.getSubject.isVariable) None else Option(triple.getSubject.toString),
      if(triple.getPredicate.isVariable) None else Option(triple.getPredicate.toString),
      if(triple.getObject.isVariable) None else Option(triple.getObject.toString)
    )
  }

//  def find (s: Node, p: Node, o: Node) = {
//    val targetTriple = Triple.createMatch(s, p, o)
//    triples.filter(t => t.getur)
//  }

}
