package org.dissect.inference.forwardchaining

import org.apache.spark.rdd.RDD
import org.dissect.inference.data.{RDFGraph, RDFTriple}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
trait ForwardRuleReasoner {

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: RDFGraph) : RDFGraph

//  def computeTransitiveClosure[A, B, C](s: mutable.Set[(A, B, C)]): mutable.Set[(A, B, C)] = {
//    val t = addTransitive(s)
//    // recursive call if set changed, otherwise stop and return
//    if (t.size == s.size) s else computeTransitiveClosure(t)
//  }

  def computeTransitiveClosure(s: mutable.Set[RDFTriple]): mutable.Set[RDFTriple] = {
    val t = addTransitive(s)
    // recursive call if set changed, otherwise stop and return
    if (t.size == s.size) s else computeTransitiveClosure(t)
  }

//  def addTransitive[A, B, C](s: mutable.Set[(A, B, C)]) = {
//    s ++ (for ((s1, p1, o1) <- s; (s2, p2, o2) <- s if o1 == s2) yield (s1, p1, o2))
//  }

  def addTransitive(s: mutable.Set[RDFTriple]) = {
    s ++ (for (t1 <- s; t2 <- s if t1.`object` == t2.subject) yield RDFTriple(t1.subject, t1.predicate, t2.`object`))
  }

  def computeTransitiveClosure(triples: RDD[RDFTriple]): RDD[RDFTriple] = {
    if(triples.isEmpty()) return triples

    // keep the predicate
    val predicate = triples.take(1)(0).predicate

    // compute the TC
    var subjectObjectPairs = triples.map(t => (t.subject, t.`object`)).cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val objectSubjectPairs = subjectObjectPairs.map(t => (t._2, t._1))

    // the join is iterated until a fixed point is reached
    var oldCount = 0L
    var nextCount = triples.count()
    do {
      oldCount = nextCount
      // perform the join (s1, o1) x (o2, s2), obtaining an RDD of (s1=o2, (o1, s2)) pairs,
      // then project the result to obtain the new (s2, o1) paths.
      subjectObjectPairs = subjectObjectPairs
        .union(subjectObjectPairs.join(objectSubjectPairs).map(x => (x._2._2, x._2._1)))
        .distinct()
        .cache()
      nextCount = subjectObjectPairs.count()
    } while (nextCount != oldCount)

    println("TC has " + subjectObjectPairs.count() + " triples.")
    subjectObjectPairs.map(p => new RDFTriple(p._1, predicate, p._2))
  }

  def computeTransitiveClosure[A:ClassTag](edges: RDD[(A, A)]): RDD[(A, A)] = {
    // we keep the transitive closure cached
    var tc = edges
    tc.cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(t => (t._2, t._1))

    // the join is iterated until a fixed point is reached
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      oldCount = nextCount
      // perform the join (x, y) x (y, x), obtaining an RDD of (x=y, (y, x)) pairs,
      // then project the result to obtain the new (x, y) paths.
      tc = tc
        .union(tc.join(edgesReversed).map(x => (x._2._2, x._2._1)))
        .distinct()
        .cache()
      nextCount = tc.count()
    } while (nextCount != oldCount)

    println("TC has " + tc.count() + " edges.")
    tc
  }

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the triples
    * @param predicate the predicate
    * @return the set of triples that contain the predicate
    */
  def extractTriples(triples: mutable.Set[RDFTriple], predicate: String): mutable.Set[RDFTriple] = {
    triples.filter(triple => triple.predicate == predicate)
  }

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the RDD of triples
    * @param predicate the predicate
    * @return the RDD of triples that contain the predicate
    */
  def extractTriples(triples: RDD[RDFTriple], predicate: String): RDD[RDFTriple] = {
    triples.filter(triple => triple.predicate == predicate)
  }

  /**
    * Extracts all triples that match the given subject, predicate and object if defined.
    *
    * @param triples the RDD of triples
    * @param subject the subject
    * @param predicate the predicate
    * @param obj the object
    * @return the RDD of triples that match
    */
  def extractTriples(triples: RDD[RDFTriple],
                     subject: Option[String],
                     predicate: Option[String],
                     obj: Option[String]): RDD[RDFTriple] = {
    var extractedTriples = triples

    if(subject.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.subject == subject.get)
    }

    if(predicate.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.predicate == predicate.get)
    }

    if(obj.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.`object` == obj.get)
    }

    extractedTriples
  }

}
