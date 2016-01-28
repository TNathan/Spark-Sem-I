package org.dissect.inference.rules

import java.io.File

import org.apache.jena.reasoner.rulesys.Rule
import org.dissect.inference.utils.{GraphUtils, RuleUtils}

import scala.collection.JavaConversions._
import scala.language.{existentials, implicitConversions}
import scala.reflect.io.Directory
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.Graph
import org.dissect.inference.utils.GraphUtils._

/**
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphAnalyzer {

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
      val rules = Rule.parseRules(org.apache.jena.reasoner.rulesys.Util.loadRuleParserFromResourceFile(filename))

      // print each rule as graph
      rules.foreach { r =>
        val g = RuleUtils.asGraph(r).export(new File(graphDir, r.getName + ".graphml").toString)
      }

      // generate graph
      val g = RuleDependencyGraphGenerator.generate(rules.toSet)

      // analyze graph
      RuleDependencyGraphAnalyzer.analyze(g)

      // export rule dependency graph
      g.export(new File(graphDir, new File(filename).getName + ".graphml").toString)
    }

  }
}
