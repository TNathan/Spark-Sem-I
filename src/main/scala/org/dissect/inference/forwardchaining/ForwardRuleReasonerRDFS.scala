package org.dissect.inference.forwardchaining

import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.SparkContext
import org.dissect.inference.data.{RDFGraph, RDFTriple}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param sc the Apache Spark context
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFS(sc: SparkContext) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val triplesRDD = graph.triples.cache() // we cache this RDD because it's used quite often

    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here


    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf



    /**
      * rdfs11	xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz .	  xxx rdfs:subClassOf zzz .
     */
    val subClassOfTriples = extractTriples(triplesRDD, RDFS.subClassOf.getURI) // extract rdfs:subClassOf triples
    val subClassOfTriplesTrans = computeTransitiveClosure(mutable.Set()++subClassOfTriples.collect())

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */
    val subPropertyOfTriples = extractTriples(triplesRDD, RDFS.subPropertyOf.getURI) // extract rdfs:subPropertyOf triples
    val subPropertyOfTriplesTrans = computeTransitiveClosure(extractTriples(mutable.Set()++subPropertyOfTriples.collect(), RDFS.subPropertyOf.getURI))

    // a map structure should be more efficient
    val subClassOfMap = subClassOfTriplesTrans.map(t => (t.subject, t.`object`)).toMap
    val subPropertyMap = subPropertyOfTriplesTrans.map(t => (t.subject, t.`object`)).toMap

    // distribute the schema data structures by means of shared variables
    // the assumption here is that the schema is usually much smaller than the instance data
    val subClassOfMapBC = sc.broadcast(subClassOfMap)
    val subPropertyMapBC = sc.broadcast(subPropertyMap)

    // 2. SubPropertyOf inheritance according to rdfs7 is computed

    /*
      rdfs7	aaa rdfs:subPropertyOf bbb .
            xxx aaa yyy .                   	xxx bbb yyy .
     */
    val triplesRDFS7 =
      triplesRDD
      .filter(t => subPropertyMapBC.value.contains(t.predicate))
      .map(t => RDFTriple(t.subject, subPropertyMapBC.value(t.predicate), t.`object`))

    // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

    /*
    rdfs2	aaa rdfs:domain xxx .
          yyy aaa zzz .	          yyy rdf:type xxx .
     */
    val domainTriples = extractTriples(triplesRDD, RDFS.domain.getURI)
    val domainMap = domainTriples.map(t => (t.subject, t.`object`)).collect.toMap
    val domainMapBC = sc.broadcast(domainMap)

    val triplesRDFS2 =
      triplesRDD
        .filter(t => domainMapBC.value.contains(t.predicate))
        .map(t => RDFTriple(t.subject, RDF.`type`.getURI, domainMapBC.value(t.predicate)))

    /*
   rdfs3	aaa rdfs:range xxx .
         yyy aaa zzz .	          zzz rdf:type xxx .
    */
    val rangeTriples = extractTriples(triplesRDD, RDFS.range.getURI)
    val rangeMap = rangeTriples.map(t => (t.subject, t.`object`)).collect().toMap
    val rangeMapBC = sc.broadcast(rangeMap)

    val triplesRDFS3 =
      triplesRDD
        .filter(t => rangeMapBC.value.contains(t.predicate))
        .map(t => RDFTriple(t.subject, RDF.`type`.getURI, rangeMapBC.value(t.predicate)))


    // 4. SubClass inheritance according to rdfs9

    /*
    rdfs9	xxx rdfs:subClassOf yyy .
          zzz rdf:type xxx .	        zzz rdf:type yyy .
     */
    val triplesRDFS9 =
      triplesRDD
        .filter(t => t.predicate == RDF.`type`.getURI) // all rdf:type triples (s a A)
        .filter(t => subClassOfMapBC.value.contains(t.`object`)) // such that A has a super class B
        .map(t => RDFTriple(t.subject, RDF.`type`.getURI, subClassOfMapBC.value(t.`object`))) // create triple (s a B)


    // 5. merge triples and remove duplicates
    val allTriples = triplesRDFS2 union triplesRDFS3 union triplesRDFS7 union triplesRDFS9 distinct()

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")

    // return graph with inferred triples
    new RDFGraph(allTriples)
  }
}
