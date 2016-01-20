package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.dissect.inference.utils.RuleUtils

import scala.collection.JavaConversions._
import scala.language.{existentials, implicitConversions}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.Graph

/**
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphAnalyzer {

  def analyze(g: Graph[Rule, DiEdge]) = {
    // check for cycles
    val cycle = g.findCycle
    println("Cycle found: " + cycle.nonEmpty)

    // topological sort
    g.topologicalSort.fold(
      cycleNode => println("Cycle detected: " + cycleNode.value.getName),
      _.toLayered foreach { layer =>
        println("---" * 3 + "layer " + layer._1 + "---" * 3)
        layer._2.foreach(node => {
          print(node.value.getName + "(" + RuleUtils.entailmentType(node.value) + ")->" + node.diSuccessors.map(r => r.value.getName).mkString("|") + " ")
        })
        println()
      }
    )
  }


  def main(args: Array[String]) {
    // we re-use the JENA API for parsing rules
    val filename = "rdfs-simple.rules"
    val rules = Rule.parseRules(org.apache.jena.reasoner.rulesys.Util.loadRuleParserFromResourceFile(filename))

    // generate graph
    val g = RuleDependencyGraphGenerator.generate(rules.toSet)

    // analyze graph
    RuleDependencyGraphAnalyzer.analyze(g)
  }
}
