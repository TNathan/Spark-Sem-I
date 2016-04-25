package org.dissect.inference.rules.plan

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dissect.inference.data.RDFGraphDataFrame
import org.slf4j.LoggerFactory

/**
  * An executor that works on Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class PlanExecutorSQL(sqlContext: SQLContext) extends PlanExecutor[DataFrame, RDFGraphDataFrame]{
  override val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def execute(plan: Plan, graph: RDFGraphDataFrame): RDFGraphDataFrame = {

    // generate SQL query
    val sql = plan.toSQL
    logger.info("SQL:" + sql)

    // execute the query
    val results = graph.toDataFrame().sqlContext.sql(sql)
//    println(results.explain(true))

    new RDFGraphDataFrame(results)
  }
}
