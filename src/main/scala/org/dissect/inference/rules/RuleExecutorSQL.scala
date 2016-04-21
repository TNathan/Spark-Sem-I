package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dissect.inference.data.RDFGraph
import org.dissect.inference.rules.plan.{PlanExecutorNative, PlanExecutorSQL}

/**
  * A rule executor that works on SQL and Spark DataFrames.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorSQL(sqlContext: SQLContext) {

  val planExecutor = new PlanExecutorSQL(sqlContext)

  def execute(rule: Rule, df: DataFrame): DataFrame = {

    // generate execution plan
    val plan = Planner.generatePlan(rule)

    // apply rule
    val result = planExecutor.execute(plan, df)

    result
  }
}
