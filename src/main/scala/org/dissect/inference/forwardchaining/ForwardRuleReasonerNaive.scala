package org.dissect.inference.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.dissect.inference.data.RDFGraph
import org.dissect.inference.rules.RuleExecutorNative
import org.slf4j.LoggerFactory

import scala.language.{existentials, implicitConversions}

/**
  * A naive implementation of the forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerNaive(sc: SparkContext, rules: Set[Rule]) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  val ruleExecutor = new RuleExecutorNative(sc)

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: RDFGraph): RDFGraph = {

    var currentGraph = graph.cache()

    var iteration = 0

    var oldCount = 0L
    var nextCount = currentGraph.size
    do {
      iteration += 1
      logger.debug("Iteration " + iteration)
      oldCount = nextCount

      currentGraph = new RDFGraph(
        currentGraph
        .union(applyRules(graph))
          .triples
          .distinct()
          .cache())
      nextCount = currentGraph.size()
    } while (nextCount != oldCount)

    graph
  }

  /**
    * Apply a set of rules on the given graph.
    *
    * @param graph the graph
    */
  def applyRules(graph: RDFGraph): RDFGraph = {
    var newGraph = graph
    rules.foreach {rule =>
      newGraph = newGraph.union(applyRule(rule, graph))
    }
    newGraph
  }

  /**
    * Apply a single rule on the given graph.
    *
    * @param rule the rule
    * @param graph the graph
    */
  def applyRule(rule: Rule, graph: RDFGraph): RDFGraph = {
    logger.debug("Rule:" + rule)
    ruleExecutor.execute(rule, graph)
  }
}
