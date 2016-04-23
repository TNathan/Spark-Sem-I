package org.dissect.inference.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.dissect.inference.data.RDFGraph
import org.dissect.inference.rules.{RuleDependencyGraphAnalyzer, RuleDependencyGraphGenerator, RuleExecutorNative, RuleExecutorSQL}
import org.slf4j.LoggerFactory

import scala.language.{existentials, implicitConversions}
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge

/**
  * An optimized implementation of the forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOptimizedSQL(sqlContext: SQLContext, rules: Set[Rule]) {

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  val ruleExecutor = new RuleExecutorSQL(sqlContext)

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: DataFrame): DataFrame = {

    var newGraph = graph

    // generate the rule dependency graph
    val dependencyGraph = RuleDependencyGraphGenerator.generate(rules)

    // generate the high-level dependency graph
    val highLevelDependencyGraph = RuleDependencyGraphAnalyzer.computeHighLevelDependencyGraph(dependencyGraph)

    // apply topological sort
    val layers = highLevelDependencyGraph.topologicalSort.right.get.toLayered

    // each layer contains a set of rule dependency graphs
    // for each layer we process those
    layers foreach { layer =>
      logger.info("Processing layer " + layer._1 + "---" * 10)
      logger.info(layer._2
        .map(sub => sub.value)
        .map((g: Graph[Rule, DiEdge]) => g.nodes.map(node => node.value.getName).mkString("G(", "|", ")"))
        .mkString("--"))

      layer._2.foreach{node =>
        val subgraph = node.value
        logger.info("Processing dependency graph " + subgraph.nodes.map(_.getName).mkString("G(", "|", ")"))
        newGraph = newGraph
                    .unionAll(applyRules(subgraph.nodes.map(node => node.value).toSeq, newGraph))
                    .distinct()
                    .cache()
      }
    }

    // de-duplicate
    val triples = newGraph//.distinct()

    // return new graph
    triples
  }

  /** Layers of a topological order of a graph or of an isolated graph component.
    * The layers of a topological sort can roughly be defined as follows:
    * a. layer 0 contains all nodes having no predecessors,
    * a. layer n contains those nodes that have only predecessors in anchestor layers
    * with at least one of them contained in layer n - 1
    */
  def toLayers(g: Graph[Rule, DiEdge]) = {
    g.topologicalSort.right.get.toLayered
  }

  /**
    * Apply the set of rules on the given graph by doing fix-point iteration.
    *
    * @param rules the rules
    * @param graph the graph
    */
  def applyRules(rules: Seq[Rule], graph: DataFrame): DataFrame = {
    var newGraph = graph

    var oldCount = 0L
    var nextCount = newGraph.count()
    do {
      oldCount = nextCount

      newGraph = newGraph
                  .unionAll(applyRulesOnce(rules, newGraph))
                  .distinct()
//                  .cache()
      nextCount = newGraph.count()
    } while (nextCount != oldCount)

    newGraph
  }

  /**
    * Apply the set of rules on the given graph once.
    *
    * @param rules the rules
    * @param graph the graph
    */
  def applyRulesOnce(rules: Seq[Rule], graph: DataFrame): DataFrame = {
    var newGraph = graph
    rules.foreach {rule =>
      newGraph = newGraph.unionAll(applyRule(rule, newGraph))
    }
    newGraph
  }

  /**
    * Apply a single rule on the given graph.
    *
    * @param rule the rule
    * @param graph the graph
    */
  def applyRule(rule: Rule, graph: DataFrame): DataFrame = {
    logger.debug("Rule:" + rule)
    ruleExecutor.execute(rule, graph)
  }
}
