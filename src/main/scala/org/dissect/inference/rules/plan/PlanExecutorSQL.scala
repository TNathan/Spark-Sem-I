package org.dissect.inference.rules.plan

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

/**
  * An executor that works on Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorSQL(sqlContext: SQLContext) {
  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, triplesDataFrame: DataFrame): DataFrame = {

    // generate SQL query
    val sql = plan.toSQL
    logger.info("SQL:" + sql)

    // execute the query
    val results = sqlContext.sql(sql)
//    println(results.explain(true))

    results
  }
}
