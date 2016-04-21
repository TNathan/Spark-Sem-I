package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.dissect.inference.data.RDFGraph
import org.dissect.inference.rules.plan.PlanExecutorNative

/**
  * A rule executor that works on Spark data structures and operations.
  *
  * @author Lorenz Buehmann
  */
class RuleExecutorNative(sc: SparkContext) {

  val planGenerator = Planner

  val planExecutor = new PlanExecutorNative(sc)

  def execute(rule: Rule, graph: RDFGraph): RDFGraph = {

    // generate execution plan
    val plan = planGenerator.generatePlan(rule)

    // apply rule
    val result = planExecutor.execute(plan, graph)

    result
  }
}
