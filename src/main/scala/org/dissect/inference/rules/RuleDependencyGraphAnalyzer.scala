package org.dissect.inference.rules

import java.io.File

import org.apache.jena.reasoner.rulesys.Rule
import org.dissect.inference.utils.{GraphUtils, RuleUtils}

import scala.collection.JavaConversions._
import scala.language.{existentials, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.io.Directory
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphTraversal
import scalax.collection.connectivity.GraphComponents
import scalax.collection.mutable.Graph
import org.dissect.inference.utils.GraphUtils._
import scalax.collection.connectivity.GraphComponents.graphToComponents
import scala.reflect.runtime.universe._

/**
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphAnalyzer {

  /**
    * Analyze a set of rules.
    *
    * @param rules the rules to analyze
    */
  def analyze(rules: Set[Rule]) : Unit = {

    // split into t-rules and a-rules first
    val tRules = rules.filter(RuleUtils.isTerminological)

    // generate the dependency graph for the t-rules
    val tRulesGraph = RuleDependencyGraphGenerator.generate(tRules)

    // compute the strongly connected components DAG
    val sccDag = GraphComponents.graphToComponents(tRulesGraph).stronglyConnectedComponentsDag

    // apply topological sort, i.e. we get layers of nodes where each node denotes a subgraph(i.e. set of rules)
    // and nodes in layer n have only predecessors in ancestor layers with at least one of them contained in layer n-1
    sccDag.topologicalSort.fold(
      cycle => println("Cycle detected:" + cycle),
      _.toLayered foreach { layer =>
          println("---" * 3 + "layer " + layer._1 + "---" * 3)
        layer._2.foreach(node => {
          val subgraph = node.value
          print("graph(" + subgraph.nodes.map(_.getName).mkString("|") + ")  ")
        })
        println()
      }
    )

    val aRules = rules.filter(RuleUtils.isAssertional)

    // generate the dependency graph for the a-rules
    val aRulesGraph = RuleDependencyGraphGenerator.generate(aRules)

    // compute the strongly connected components DAG
    val aRulesSccDag = GraphComponents.graphToComponents(aRulesGraph).stronglyConnectedComponentsDag

    // apply topological sort, i.e. we get layers of nodes where each node denotes a subgraph(i.e. set of rules)
    // and nodes in layer n have only predecessors in ancestor layers with at least one of them contained in layer n-1
    aRulesSccDag.topologicalSort.fold(
      cycle => println("Cycle detected:" + cycle),
      _.toLayered foreach { layer =>
        println("---" * 3 + "layer " + layer._1 + "---" * 3)
        layer._2.foreach(node => {
          val subgraph = node.value
          print("graph(" + subgraph.nodes.map(_.getName).mkString("|") + ")  ")
        })
        println()
      }
    )

    RuleDependencyGraphAnalyzer.analyze(tRulesGraph)

  }
//
//  def showLayers(topologicalOrder: GraphTraversal#LayeredTopologicalOrder[Graph[Rule, DiEdge]]) = {
//    topologicalOrder foreach { layer =>
//      println("---" * 3 + "layer " + layer._1 + "---" * 3)
//      layer._2.foreach(node => {
//        val subgraph = node.value
//        print("graph(" + subgraph.nodes.map(_.getName).mkString("|") + ")  ")
//      })
//      println()
//    }
//  }

  def analyze(g: Graph[Rule, DiEdge]) = {
    // check for cycles
    val cycle = g.findCycle
    println("Cycle found: " + cycle.nonEmpty)
    println(cycle.getOrElse(println))

    // topological sort
    g.topologicalSort.fold(
      cycleNode => println("Cycle detected: " + cycleNode.value.getName),
      _.toLayered foreach { layer =>
        println("---" * 3 + "layer " + layer._1 + "---" * 3)
        layer._2.foreach(node => {
          val rule = node.value
          val ruleType = RuleUtils.entailmentType(rule)
          val isTC = RuleUtils.isTransitiveClosure(rule)
          print(rule.getName + "(" + ruleType + (if (isTC) ", TC" else "")  + ")->" + node.diSuccessors.map(r => r.value.getName).mkString("|") + " ")
        })
        println()
      }
    )
  }


  def main(args: Array[String]) {
    // we re-use the JENA API for parsing rules
    val filenames = List(
//      "rules/rdfs-simple.rules"
      "rules/owl_horst.rules"
//    "rules/owl_rl.rules"
    )

    val graphDir = new File("graph")
    graphDir.mkdir()


    filenames.foreach { filename =>
      println(filename)

      // parse the rules
      val rules = RuleUtils.load(filename).toSet

      // print each rule as graph
      rules.foreach { r =>
        val g = RuleUtils.asGraph(r).export(new File(graphDir, r.getName + ".graphml").toString)
      }

      // generate graph
      val g = RuleDependencyGraphGenerator.generate(rules.toSet)

      // analyze graph
      RuleDependencyGraphAnalyzer.analyze(rules)

      // export rule dependency graph
      g.export(new File(graphDir, new File(filename).getName + ".graphml").toString)
    }

  }
}
