package org.dissect.inference.utils

import org.apache.jena.vocabulary.RDFS
import org.dissect.inference.data.{AbstractRDFGraph, RDFGraph, RDFTriple}

/**
  * @author Lorenz Buehmann
  */
class SchemaExtractor {



  def extract(graph: AbstractRDFGraph) = {

    val properties = List(RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range)

    properties.foreach{p =>
      graph.find(None, Some(p.getURI), None)
    }
    // extract rdfs:subClassOf triples

  }

}


