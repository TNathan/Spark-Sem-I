package org.dissect.inference.rules

import org.apache.spark.rdd.RDD
import org.dissect.inference.data.RDFTriple

/**
  * Common operations on RDD of triples.
  *
  * @author Lorenz Buehmann
  */
object RDDOperations {


  /**
    * For a given triple (s,p,o) it returns (s,o)
    * @param triples the triples (s,p,o)
    * @return tuples (s,o)
    */
  def subjObj(triples: RDD[RDFTriple]) = {
    triples.map(t => (t.subject, t.`object`))
  }

  /**
    * For a given triple (s,p,o) it returns (o,s)
    * @param triples the triples (s,p,o)
    * @return tuples (o,s)
    */
  def objSubj(triples: RDD[RDFTriple]) = {
    triples.map(t => (t.`object`, t.subject))
  }

  /**
    * For a given triple (s,p,o) it returns (p,(s,o))
    * @param triples the triples (s,p,o)
    * @return tuples (p,(s,o))
    */
  def predKeySubjObj(triples: RDD[RDFTriple]) = {
    triples.map(t => t.predicate -> (t.subject, t.`object`))
  }

  /**
    * For a given triple (s,p,o) it returns (p,(o,s))
    * @param triples the triples (s,p,o)
    * @return tuples (p,(o,s))
    */
  def predKeyObjSubj(triples: RDD[RDFTriple]) = {
    triples.map(t => t.predicate -> (t.`object`, t.subject))
  }

  /**
    * For a given tuple (x,y) it returns (y,x)
    * @param tuples the tuples (x,y)
    * @return tuples (y,x)
    */
  def swap[A,B](tuples: RDD[(A, B)]) = {
    tuples.map(t => (t._2, t._1))
  }

}