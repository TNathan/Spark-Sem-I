package org.dissect.inference.rules.plan

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.optimizer.DefaultOptimizer
import org.apache.spark.sql.catalyst.{DefaultParserDialect, ParserDialect, SqlParser}
import org.apache.spark.sql.execution.SparkSQLParser
import org.apache.spark.sql.execution.datasources.DDLParser
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.Utils

/**
  * An executor that works on Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorSQL(sc: SparkContext) {
  def execute(plan: Plan, triplesDataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    // generate SQL query
    val sql = plan.toSQL

    println(sql)

    val sqlParser = new SparkSQLParser(SqlParser.parse(_))
    val ddlParser = new DDLParser(sqlParser.parse(_))

    val logicalPlan = ddlParser.parse(sql, false)
    println(logicalPlan.toString())

    val optimizedPlan = DefaultOptimizer.execute(logicalPlan)
    println(optimizedPlan.toString())

    val results = sqlContext.sql(sql)

//    println(results.explain(true))

//    results.collect().foreach(println)

//    val sql2 =
//      "SELECT rel1.subject, rel0.subject, rel2.object " +
//        "FROM " +
//        "(SELECT subject FROM TRIPLES WHERE predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AND object='http://www.w3.org/2002/07/owl#TransitiveProperty') rel0, " +
//        "TRIPLES rel1, " +
//        "TRIPLES rel2 " +
//      " WHERE rel1.object=rel2.subject AND rel0.subject=rel2.predicate AND rel0.subject=rel1.predicate AND rel1.predicate=rel2.predicate"
//    println(sql2)
//    val results2 = sqlContext.sql(sql2)
//    println(results2.explain(true))
//
////    results2.collect().foreach(println)
//
//    val sql3 =
//      "SELECT rel1.subject, rel0.subject, rel2.object " +
//        "FROM " +
//        "(SELECT subject FROM TRIPLES WHERE predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AND object='http://www.w3.org/2002/07/owl#TransitiveProperty') rel0, " +
//        "TRIPLES rel1, " +
//        "TRIPLES rel2 " +
//        " WHERE rel1.object=rel2.subject AND rel0.subject=rel2.predicate AND rel1.predicate=rel2.predicate"
//    println(sql3)
//    val results3 = sqlContext.sql(sql3)
//    println(results3.explain(true))

//    results3.collect().foreach(println)

    results
  }
}
