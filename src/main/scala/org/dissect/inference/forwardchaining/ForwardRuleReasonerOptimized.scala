package org.dissect.inference.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.dissect.inference.data.RDFGraph
import org.dissect.inference.rules._
import org.slf4j.LoggerFactory

import scala.language.{existentials, implicitConversions}
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge

/**
  * An optimized implementation of the forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimized(sc: SparkContext, rules: Set[Rule]) extends ForwardRuleReasoner{

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

    var newGraph = graph.cache()

    // generate the rule dependency graph
    val dependencyGraph = RuleDependencyGraphGenerator.generate(rules)

    // generate the high-level dependency graph
    val highLevelDependencyGraph = HighLevelRuleDependencyGraphGenerator.generate(dependencyGraph)

    // apply topological sort and get the layers
    val layers = highLevelDependencyGraph.layers()

    // each layer contains a set of rule dependency graphs
    // for each layer we process those
    layers foreach { layer =>
      newGraph = newGraph.union(processLayer(layer, graph))
    }

    // de-duplicate
    val triples = newGraph.triples.distinct()

    // return new graph
    new RDFGraph(triples)
  }

  def processLayer(layer: (Int, Iterable[RuleDependencyGraph]), graph: RDFGraph) = {
    logger.info("Processing layer " + layer._1 + "---" * 10)
    logger.info(layer._2.map(rdg => rdg.printNodes()).mkString("--"))

    var newGraph = graph

    layer._2.foreach{rdg =>
      logger.info("Processing dependency graph " + rdg.printNodes())
      newGraph = newGraph.union(
        new RDFGraph(
          applyRules(rdg.rules().toSeq, newGraph)
            .triples
            .distinct()
            .cache())
      )
    }
    newGraph
  }

  /**
    * Apply the set of rules on the given graph by doing fix-point iteration.
    *
    * @param rules the rules
    * @param graph the graph
    */
  def applyRules(rules: Seq[Rule], graph: RDFGraph): RDFGraph = {
    var newGraph = graph

    var oldCount = 0L
    var nextCount = newGraph.size
    do {
      oldCount = nextCount

      newGraph = new RDFGraph(
        newGraph
          .union(applyRulesOnce(rules, graph))
          .triples
          .distinct()
          .cache())
      nextCount = newGraph.size()
    } while (nextCount != oldCount)

    graph

  }

  /**
    * Apply the set of rules on the given graph once.
    *
    * @param rules the rules
    * @param graph the graph
    */
  def applyRulesOnce(rules: Seq[Rule], graph: RDFGraph): RDFGraph = {
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
