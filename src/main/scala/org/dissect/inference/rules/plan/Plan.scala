package org.dissect.inference.rules.plan

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.dissect.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * An execution plan to process a single rule.
  *
  * @author Lorenz Buehmann
  */
case class Plan(triplePatterns: Set[Triple], target: Triple, joins: mutable.Set[Join]) {

  val aliases = new mutable.HashMap[Triple, String]()
  var idx = 0

  def generateJoins() = {

  }

  def addTriplePattern(tp: TriplePattern) = {

  }

  def toSQL = {
    var sql = "SELECT "

    val requiredVars = RuleUtils.varsOf(target)

    sql += fromPart()

    sql += wherePart()

    sql
  }

  def fromPart(): String = {
    " FROM " + triplePatterns.map(tp => fromPart(tp)).mkString(", ")
  }

  def wherePart(): String = {
    var sql = " WHERE "
    val expressions = mutable.ArrayBuffer[String]()

    expressions ++= triplePatterns.flatMap(tp => whereParts(tp))
    expressions ++= joins.map(join => joinExpressionFor(join))

    sql += expressions.mkString(" AND ")

    sql
  }

  def toSQL(tp: Triple) = {
    var sql = "SELECT "

    sql += projectionPart(tp)

    sql += " FROM " + fromPart(tp)

    sql += " WHERE " + whereParts(tp).mkString(" AND ")

    sql
  }

  def projectionPart(tp: Triple) = {
    subjectColumn() + ", " + predicateColumn() + ", " + objectColumn()
  }

  def projectionPart(tp: Triple, selectedVars: List[Node]) = {

  }

  def uniqueAliasFor(tp: Triple): String = {
    aliases.get(tp) match {
      case Some(alias) => alias
      case _ =>
        val alias = "rel" + idx
        aliases += tp -> alias
        idx += 1
        alias
    }
  }

  def joinExpressionFor(tp1: Triple, tp2: Triple, joinVar: Node) = {
    expressionFor(joinVar, tp1) + "=" + expressionFor(joinVar, tp2)
  }

  def joinExpressionFor(join: Join) = {
    expressionFor(join.joinVar, join.tp1) + "=" + expressionFor(join.joinVar, join.tp2)
  }

  def fromPart(tp: Triple) = {
    tableName(tp)
  }

  def expressionFor(variable: Node, tp: Triple): String = {
    val ret =
      if (tp.subjectMatches(variable)) {
        subjectColumnName(tp)
      } else if (tp.predicateMatches(variable)) {
        predicateColumnName(tp)
      } else if (tp.objectMatches(variable)) {
        objectColumnName(tp)
      } else {
        "NULL"
      }
    ret
  }

  def isVarWithName(node: Node) = {

  }

  def whereParts(tp: Triple) = {
    val res = mutable.Set[String]()

    if(!tp.getSubject.isVariable) {
      res += subjectColumnName(tp) + "='" + tp.getSubject + "'"
    }

    if(!tp.getPredicate.isVariable) {
      res += predicateColumnName(tp) + "='" + tp.getPredicate + "'"
    }

    if(!tp.getObject.isVariable) {
      res += objectColumnName(tp) + "='" + tp.getObject + "'"
    }
    res
  }

  def subjectColumnName(tp: Triple) = {
    uniqueAliasFor(tp) + "." + subjectColumn()
  }

  def predicateColumnName(tp: Triple) = {
    uniqueAliasFor(tp) + "." + predicateColumn()
  }

  def objectColumnName(tp: Triple) = {
    uniqueAliasFor(tp) + "." + objectColumn()
  }

  def tableName(tp: Triple) = {
    table() + " " + uniqueAliasFor(tp)
  }

  def table() = {
    "TRIPLES"
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
