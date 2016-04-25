package org.dissect.inference.rules

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dissect.inference.data.RDFGraphDataFrame
import org.dissect.inference.rules.plan.PlanExecutorSQL

/**
  * A rule executor that works on SQL and Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorSQL(sqlContext: SQLContext) extends RuleExecutor[DataFrame, RDFGraphDataFrame](new PlanExecutorSQL(sqlContext)){

}
