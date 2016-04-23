package org.dissect.inference.rules.plan

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.optimizer.DefaultOptimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SparkSQLParser}
import org.apache.spark.sql.execution.datasources.DDLParser
import org.dissect.inference.utils.{RuleUtils, TripleUtils}

import scala.collection.mutable

/**
  * An execution plan to process a single rule.
  *
  * @author Lorenz Buehmann
  */
case class Plan(triplePatterns: Set[Triple], target: Triple, joins: mutable.Set[Join]) {

  val sqlParser = new SparkSQLParser(SqlParser.parse(_))
  val ddlParser = new DDLParser(sqlParser.parse(_))

  val aliases = new mutable.HashMap[Triple, String]()
  var idx = 0

  def generateJoins() = {

  }

  def addTriplePattern(tp: TriplePattern) = {

  }

  def toLogicalPlan(sqlContext: SQLContext): LogicalPlan = {
    // convert to SQL query
    val sql = toSQL

    // generate logical plan
    var logicalPlan = ddlParser.parse(sql, false)
//    println(logicalPlan.toString())

    // optimize plan
    logicalPlan = DefaultOptimizer.execute(logicalPlan)
//    println(logicalPlan.toString())

    val qe = new QueryExecution(sqlContext, logicalPlan)
    val optimizedPlan = DefaultOptimizer.execute(qe.optimizedPlan)

    optimizedPlan
  }

  def toSQL = {
    var sql = "SELECT "

    sql += projectionPart()

    sql += fromPart()

    sql += wherePart()

    sql
  }

  def projectionPart(): String = {
    var sql = ""

    val requiredVars = TripleUtils.nodes(target)

    val expressions = mutable.ArrayBuffer[String]()

//    expressions += (if(target.getSubject.isVariable) expressionFor(target.getSubject, target) else target.getSubject.toString)
//    expressions += (if(target.getPredicate.isVariable) expressionFor(target.getPredicate, target) else target.getPredicate.toString)
//    expressions += (if(target.getObject.isVariable) expressionFor(target.getObject, target) else target.getObject.toString)

    requiredVars.foreach{ v =>
      if(v.isVariable) {
        var done = false

        for(tp <- triplePatterns; if !done) {
          val expr = expressionFor(v, tp)

          if(expr != "NULL") {
            expressions += expr
            done = true
          }
        }
      } else {
        expressions += "'" + v.toString + "'"
      }
    }

    sql += expressions.mkString(", ")

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
