package org.dissect.inference.rules

import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.RDFS

import scala.collection.JavaConversions._
import scala.language.{existentials, implicitConversions}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

/**
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphGenerator {

  def generate(rules: Set[Rule]): Graph[Rule, DiEdge] = {
    // create empty graph
    val g = Graph[Rule, DiEdge]()

    // add edge for each rule r1 that depends on another rule r2
    for (r1 <- rules; r2 <- rules) {
      if (dependsOn(r1, r2))
        g += r1 ~> r2
      else if (dependsOn(r2, r1))
        g += r2 ~> r1
      else if (dependsOn(r1, r1))
        g += r1 ~> r1
    }

    g
  }

  def dependsOn(rule1: Rule, rule2: Rule) : Boolean = {
    // body of rule1 has correlation with head of rule2
    val head = rule2.getHead
    val body = rule1.getBody

    var ret = false

    for (tp1 <- head.collect{case x:TriplePattern=>x}; tp2 <- body.collect{case x:TriplePattern=>x}) {
      if (tp1.getPredicate.equals(tp2.getPredicate)) ret = true
      else {
        if(tp2.getPredicate.isVariable && tp1.getPredicate.equals(RDFS.subPropertyOf.asNode())) {
          ret = true
        }
      }

    }

    ret
  }

  def main(args: Array[String]) {

    // we re-use the JENA API for parsing rules
    val filename = "rdfs-simple.rules"
    val rules = Rule.parseRules(org.apache.jena.reasoner.rulesys.Util.loadRuleParserFromResourceFile(filename))

    // generate graph
    val g = RuleDependencyGraphGenerator.generate(rules.toSet)

    // check for cycles
    val cycle = g.findCycle
    println("Cycle found: " + cycle.nonEmpty)

    // topological sort
    g.topologicalSort.fold(
      cycleNode => println("Cycle detected: " + cycleNode.value.getName),
      _.toLayered foreach { layer =>
        println("---" * 3 + "layer " + layer._1 + "---" * 3)
        layer._2.foreach(node => print(node.value.getName + " "))
        println()
      }
    )

  }
}
