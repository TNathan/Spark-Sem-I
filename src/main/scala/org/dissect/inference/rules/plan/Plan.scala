package org.dissect.inference.rules.plan

import org.apache.jena.graph.Triple
import org.apache.jena.reasoner.TriplePattern

import scala.collection.mutable

/**
  * An execution plan to process a single rule.
  *
  * @author Lorenz Buehmann
  */
case class Plan(triplePatterns: Set[Triple], target: Triple, joins: mutable.Set[Join]) {

  def generateJoins() = {

  }

  def addTriplePattern(tp: TriplePattern) = {

  }

  def toSQL = {

  }

  def fromPart() = {
    var sql = ""

  }

  def wherePart() = {

  }

  def toSQL(tp: TriplePattern) = {
    var sql = "SELECT "

  }

  def wherePart(tp: TriplePattern) = {
    var sql = "WHERE "

    if(!tp.getSubject.isVariable) {
      sql += subjectColumn() + "=" + tp.getSubject + " AND "
    }

    if(!tp.getPredicate.isVariable) {
      sql += predicateColumn() + "=" + tp.getPredicate + " AND "
    }

    if(!tp.getObject.isVariable) {
      sql += objectColumn() + "=" + tp.getObject
    }
  }

  def subjectColumn() = {
    "subject"
  }

  def predicateColumn() = {
    "predicate"
  }

  def objectColumn() = {
    "object"
  }


}
