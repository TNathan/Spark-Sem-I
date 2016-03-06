package org.dissect.inference.forwardchaining

import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dissect.inference.data.{RDFGraph, RDFTriple}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * A forward chaining implementation of the OWL Horst entailment regime.
  *
  * @constructor create a new OWL Horst forward chaining reasoner
  * @param sc the Apache Spark context
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOWLHorst(sc: SparkContext) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val triplesRDD = graph.triples.cache() // we cache this RDD because it's used quite often

    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here


    // extract the schema data
    val subClassOfTriples = extractTriples(triplesRDD, RDFS.subClassOf.getURI) // rdfs:subClassOf
    val subPropertyOfTriples = extractTriples(triplesRDD, RDFS.subPropertyOf.getURI) // rdfs:subPropertyOf
    val domainTriples = extractTriples(triplesRDD, RDFS.domain.getURI) // rdfs:domain
    val rangeTriples = extractTriples(triplesRDD, RDFS.range.getURI) // rdfs:range


    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf

    /**
      * rdfs11	xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz .	  xxx rdfs:subClassOf zzz .
     */

    val subClassOfTriplesTrans = computeTransitiveClosure(mutable.Set()++subClassOfTriples.collect())

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */

    val subPropertyOfTriplesTrans = computeTransitiveClosure(extractTriples(mutable.Set()++subPropertyOfTriples.collect(), RDFS.subPropertyOf.getURI))


    // we put all into maps which should be more efficient later on
    val subClassOfMap = subClassOfTriplesTrans.map(t => (t.subject, t.`object`)).toMap
    val subPropertyMap = subPropertyOfTriplesTrans.map(t => (t.subject, t.`object`)).toMap
    val domainMap = domainTriples.map(t => (t.subject, t.`object`)).collect.toMap
    val rangeMap = rangeTriples.map(t => (t.subject, t.`object`)).collect().toMap

    // distribute the schema data structures by means of shared variables
    // the assumption here is that the schema is usually much smaller than the instance data
    val subClassOfMapBC = sc.broadcast(subClassOfMap)
    val subPropertyMapBC = sc.broadcast(subPropertyMap)
    val domainMapBC = sc.broadcast(domainMap)
    val rangeMapBC = sc.broadcast(rangeMap)

    // we also extract properties with certain OWL characteristic and share them
    val transitivePropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.TransitiveProperty.getURI))
        .map(triple => triple.subject)
        .collect())
    val functionalPropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.FunctionalProperty.getURI))
        .map(triple => triple.subject)
        .collect())
    val inverseFunctionalPropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.InverseFunctionalProperty.getURI))
        .map(triple => triple.subject)
        .collect())
    val symmetricPropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.SymmetricProperty.getURI))
        .map(triple => triple.subject)
        .collect())

    // and inverse property definitions
    val inverseOfMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.inverseOf.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val inverseOfMapRevertedBC = sc.broadcast(
      inverseOfMapBC.value.map(_.swap)
    )

    // and more OWL vocabulary used in property restrictions
    val someValuesFromMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.someValuesFrom.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val someValuesFromMapReversedBC = sc.broadcast(
      someValuesFromMapBC.value.map(_.swap)
    )
    val allValuesFromMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.allValuesFrom.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val allValuesFromMapReversedBC = sc.broadcast(
      allValuesFromMapBC.value.map(_.swap)
    )
    val hasValueMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.hasValue.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val onPropertyMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.onProperty.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val onPropertyMapReversedBC = sc.broadcast(
      onPropertyMapBC.value.groupBy(_._2).mapValues(_.keys)
    )
    val hasValueMapReversedBC = sc.broadcast(
      hasValueMapBC.value.groupBy(_._2).mapValues(_.keys)
    )


    // owl:sameAs is computed separately, thus, we split the data
    val triplesFiltered = triplesRDD.filter(triple => triple.predicate != OWL2.sameAs.getURI && triple.predicate == RDF.`type`.getURI)
    val sameAsTriples = triplesRDD.filter(triple => triple.predicate == OWL2.sameAs.getURI)
    val typeTriples = triplesRDD.filter(triple => triple.predicate == RDF.`type`.getURI)

    val newDataInferred = true

    while(newDataInferred) {
      // 2. SubPropertyOf inheritance according to rdfs7 is computed

      /*
        rdfs7	aaa rdfs:subPropertyOf bbb .
              xxx aaa yyy .                   	xxx bbb yyy .
       */
      val triplesRDFS7 =
        triplesFiltered
          .filter(t => subPropertyMapBC.value.contains(t.predicate))
          .map(t => RDFTriple(t.subject, subPropertyMapBC.value(t.predicate), t.`object`))

      // add the inferred triples to the existing triples
      val rdfs7Res = triplesRDFS7.union(triplesFiltered)

      // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

      /*
      rdfs2	aaa rdfs:domain xxx .
            yyy aaa zzz .	          yyy rdf:type xxx .
       */
      val triplesRDFS2 =
        rdfs7Res
          .filter(t => domainMapBC.value.contains(t.predicate))
          .map(t => RDFTriple(t.subject, RDF.`type`.getURI, domainMapBC.value(t.predicate)))

      /*
     rdfs3	aaa rdfs:range xxx .
           yyy aaa zzz .	          zzz rdf:type xxx .
      */
      val triplesRDFS3 =
        rdfs7Res
          .filter(t => rangeMapBC.value.contains(t.predicate))
          .map(t => RDFTriple(t.`object`, RDF.`type`.getURI, rangeMapBC.value(t.predicate)))


      // 4. SubClass inheritance according to rdfs9
      // input are the rdf:type triples from RDFS2/RDFS3 and the ones contained in the original graph

      /*
      rdfs9	xxx rdfs:subClassOf yyy .
            zzz rdf:type xxx .	        zzz rdf:type yyy .
       */
      val triplesRDFS9 =
        triplesRDFS2
          .union(triplesRDFS3)
          .union(typeTriples)
          .filter(t => subClassOfMapBC.value.contains(t.`object`)) // such that A has a super class B
          .map(t => RDFTriple(t.subject, RDF.`type`.getURI, subClassOfMapBC.value(t.`object`))) // create triple (s a B)


      // rdfp14b: (?R owl:hasValue ?V),(?R owl:onProperty ?P),(?X rdf:type ?R ) -> (?X ?P ?V )
      val triplesClsHv1 = typeTriples
        .filter(triple =>
          hasValueMapBC.value.contains(triple.`object`) &&
          onPropertyMapBC.value.contains(triple.`object`)
        )
        .map(triple =>
          RDFTriple(triple.subject, onPropertyMapBC.value(triple.`object`), hasValueMapBC.value(triple.`object`))
        )

      // rdfp14a: (?R owl:hasValue ?V), (?R owl:onProperty ?P), (?U ?P ?V) -> (?U rdf:type ?R)
      val triplesClsHv2 = rdfs7Res
        .filter(triple => {
          if (onPropertyMapReversedBC.value.contains(triple.predicate)) {
            // there is any restriction R for property P

            var valueRestrictionExists = false

            onPropertyMapReversedBC.value(triple.predicate).foreach { restriction =>
              if (hasValueMapBC.value.contains(restriction) && // R a hasValue restriction
                hasValueMapBC.value(restriction) == triple.`object`) {
                //  with value V
                valueRestrictionExists = true
              }

            }
            valueRestrictionExists
          }
          false
        })
        .map(triple => {

          val s = triple.subject
          val p = RDF.`type`
          var o = ""
          onPropertyMapReversedBC.value(triple.predicate).foreach { restriction => // get the restriction R
            if (hasValueMapBC.value.contains(restriction) && // R a hasValue restriction
              hasValueMapBC.value(restriction) == triple.`object`) { //  with value V

              o = restriction
            }

          }
          (s, p, o)
        }
        )

      // rdfp8a: (?P owl:inverseOf ?Q), (?X ?P ?Y) -> (?Y ?Q ?X)
      val rdfp8a = triplesFiltered
        .filter(triple => inverseOfMapBC.value.contains(triple.predicate))
        .map(triple => RDFTriple(triple.`object`, inverseOfMapBC.value(triple.predicate), triple.subject))

      // rdfp8b: (?P owl:inverseOf ?Q), (?X ?Q ?Y) -> (?Y ?P ?X)
      val rdfp8b = triplesFiltered
        .filter(triple => inverseOfMapRevertedBC.value.contains(triple.predicate))
        .map(triple => RDFTriple(triple.`object`, inverseOfMapRevertedBC.value(triple.predicate), triple.subject))

      // rdfp3: (?P rdf:type owl:SymmetricProperty), (?X ?P ?Y) -> (?Y ?P ?X)
      val rdfp3 = triplesFiltered
        .filter(triple => symmetricPropertiesBC.value.contains(triple.predicate))
        .map(triple => RDFTriple(triple.`object`, triple.predicate, triple.subject))

      // rdfp15: (?R owl:someValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?A), (?A rdf:type ?D ) -> (?X rdf:type ?R )
      val rdfp15_1 = triplesFiltered
        .filter(triple => onPropertyMapReversedBC.value.contains(triple.predicate)) // && someValuesFromMapBC.value.contains(onPropertyMapReversedBC.value(triple.predicate)))
        .map(triple => {
          val restrictions = onPropertyMapReversedBC.value(triple.predicate)
          restrictions.map(_r => ((_r -> triple.`object`), triple.subject)) // -> ((?R, ?A), ?X)
         })
        .flatMap(identity)

      val rdfp15_2 = typeTriples
        .filter(triple => someValuesFromMapReversedBC.value.contains(triple.`object`))
        .map(triple => ((someValuesFromMapReversedBC.value(triple.`object`), triple.subject), Nil)) // -> ((?R, ?A), NIL)

      val rdfp15 = rdfp15_1
        .join(rdfp15_2)
        .map(e => (e._2._1, RDF.`type`.getURI, e._1._1)) // -> (?X rdf:type ?R )


      // rdfp16: (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?Y), (?X rdf:type ?R ) -> (?Y rdf:type ?D )
      val rdfp16_1 = triplesFiltered // (?X ?P ?Y)
        .filter(triple => onPropertyMapReversedBC.value.contains(triple.predicate) &&
                          allValuesFromMapBC.value.keySet.intersect(onPropertyMapReversedBC.value(triple.predicate).toSet).nonEmpty) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .map(triple => {
          val restrictions = onPropertyMapReversedBC.value(triple.predicate)
          restrictions.map(_r => ((triple.subject -> _r), triple.`object`)) // -> ((?X, ?R), ?Y)
        })
        .flatMap(identity)

      val rdfp16_2 = typeTriples // (?X rdf:type ?R )
        .filter(triple => allValuesFromMapBC.value.contains(triple.`object`) && onPropertyMapBC.value.contains(triple.`object`)) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .map(triple => ((triple.subject, onPropertyMapBC.value(triple.`object`)), allValuesFromMapBC.value(triple.`object`))) // -> ((?X, ?R), ?D)

      val rdfp16 = rdfp16_1
        .join(rdfp16_2) // (?X, ?R), (?Y, ?D)
        .map(e => (e._2._1, RDF.`type`.getURI, e._2._2)) // -> (?Y rdf:type ?D )


    }

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")






    // rdfp15: (?R owl:someValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?A), (?A rdf:type ?D ) -> (?X rdf:type ?R )
    def rdfp15(triples: RDD[RDFTriple]) : RDD[RDFTriple] = {

      val rdfp15_1 = triples // (?X ?P ?A)
        .filter(triple => onPropertyMapReversedBC.value.contains(triple.predicate)) // (?X ?P ?A), (?R owl:onProperty ?P)
        .map(triple => {
          val restrictions = onPropertyMapReversedBC.value(triple.predicate)
          restrictions.map(_r => ((_r -> triple.`object`), triple.subject))
         })
        .flatMap(identity) // -> ((?R, ?A), ?X)

      val rdfp15_2 = typeTriples // (?A rdf:type ?D )
        .filter(triple => someValuesFromMapReversedBC.value.contains(triple.`object`)) // (?A rdf:type ?D ), (?R owl:someValuesFrom ?D)
        .map(triple => ((someValuesFromMapReversedBC.value(triple.`object`), triple.subject), Nil)) // -> ((?R, ?A), NIL)

      val rdfp15 = rdfp15_1
        .join(rdfp15_2)
        .map(e => RDFTriple(e._2._1, RDF.`type`.getURI, e._1._1)) // -> (?X rdf:type ?R )

      rdfp15
    }

    // return graph with inferred triples
    graph
  }
}
