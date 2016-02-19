package org.dissect.inference.data

/**
  * An RDF triple.
  *
  * @author Lorenz Buehmann
  */
case class RDFTriple(subject: String, predicate: String, obj: String) extends Product3[String, String, String] {
  override def _1: String = subject

  override def _3: String = predicate

  override def _2: String = obj
}
