package org.dissect.inference.utils

import org.apache.jena.graph.Triple
import org.apache.jena.vocabulary.OWL2._
import org.apache.jena.vocabulary.RDFS._
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}

/**
  * Utility class for triples.
  *
  * @author Lorenz Buehmann
  */
object TripleUtils {

  // set of properties that indicate terminological triples
  val properties = List(
    subClassOf, equivalentClass, disjointWith,
    intersectionOf, unionOf, complementOf, someValuesFrom, allValuesFrom, hasValue,
    maxCardinality, minCardinality, cardinality,
    subPropertyOf, equivalentProperty, propertyDisjointWith, domain, range, inverseOf).map(t => t.asNode())

  // set of types that indicate terminological triples
  val types = Set(
    ObjectProperty, DatatypeProperty,
    FunctionalProperty, InverseFunctionalProperty,
    SymmetricProperty, AsymmetricProperty,
    ReflexiveProperty, IrreflexiveProperty, TransitiveProperty,
    OWL2.Class , RDFS.Class, Restriction
  ).map(t => t.asNode())

  /**
    * Checks whether a triple is terminological.
    * <p>
    * A terminological triple denotes information about the TBox of the ontology (class and property axioms),
    * such as subclass relationships, class equivalence, property types, etc.
    * </p>
    *
    * @param triple the triple to check
    */
  def isTerminological(triple: Triple) : Boolean = {
    properties.contains(triple.getPredicate) ||
      (triple.getPredicate.equals(RDF.`type`) && types.contains(triple.getObject))
  }

  /**
    * Checks whether a triple is assertional.
    * <p>
    * Intuitively, an assertional triple denotes information about the ABox of the ontology, such as
    * instance class membership or instance equality/inequality (owl:sameAs/owl:differentFrom)
    * (we consider the oneOf construct as simple C(a) assertions).
    * </p>
    * @param triple the triple to check
    */
  def isAssertional(triple: Triple) : Boolean = {
    !isTerminological(triple)
  }

}
