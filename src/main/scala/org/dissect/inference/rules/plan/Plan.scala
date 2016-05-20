package org.dissect.inference.rules.plan

import java.lang.reflect.Method

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FindDataSourceTable, PreInsertCastAndRename, ResolveDataSource}
import org.apache.spark.sql.execution.{QueryExecution, SparkSqlParser, datasources}
import org.apache.spark.sql.internal.SQLConf
import org.dissect.inference.utils.TripleUtils

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

  def toLogicalPlan(sqlContext: SQLContext): LogicalPlan = {
    // convert to SQL query
    val sql = toSQL
    println("SQL query:" + sql)

    // generate logical plan
    val m = sqlContext.getClass().getDeclaredMethod("parseSql", classOf[String])
    m.setAccessible(true)
    var logicalPlan: LogicalPlan = m.invoke(sqlContext, sql).asInstanceOf[LogicalPlan]

    val session = sqlContext.sparkSession
    val m2 = session.getClass().getDeclaredMethod("sessionState")
    m2.setAccessible(true)
    val sessionState = m2.invoke(session)

    val m3 = sessionState.getClass().getDeclaredMethod("analyzer")
    m3.setAccessible(true)
    val analyzer = m3.invoke(sessionState).asInstanceOf[Analyzer]

    logicalPlan = analyzer.execute(logicalPlan)

//    println(logicalPlan.toString())

    // optimize plan
//    logicalPlan = DefaultOptimizer.execute(logicalPlan)
//    println(logicalPlan.toString())

//    val qe = new QueryExecution(sqlContext, logicalPlan)
    val optimizedPlan = logicalPlan//DefaultOptimizer.execute(qe.optimizedPlan)

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
    var sql = " FROM " + triplePatterns.map(tp => fromPart(tp)).mkString(" INNER JOIN ")
    sql += " ON " + joins.map(join => joinExpressionFor(join)).mkString(" AND ")
    sql
  }

  def wherePart(): String = {
    var sql = " WHERE "
    val expressions = mutable.ArrayBuffer[String]()

    expressions ++= triplePatterns.flatMap(tp => whereParts(tp))
//    expressions ++= joins.map(join => joinExpressionFor(join))

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
