package org.dissect.inference.rules

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.sparql.syntax.{ElementGroup, PatternVars}
import org.apache.spark.rdd.RDD
import org.dissect.inference.data.RDFTriple
import org.dissect.inference.utils.RuleUtils._
import org.dissect.inference.utils.{RuleUtils, TriplePatternOrdering}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scalax.collection.GraphTraversal.{Predecessors, Successors}

/**
  * @author Lorenz Buehmann
  */
object Planner {

  /**
    * Generates an execution plan for a single rule.
    *
    * @param rule the rule
    */
  def rewrite(rule: Rule) = {
    println("Rule: " + rule)
    val body = collection.mutable.SortedSet[TriplePattern]()(new TriplePatternOrdering()) ++ rule.bodyTriplePatterns.toSet

    // take first triple pattern
    val currentTp = body.head
    println("TP:" + currentTp)
    body.remove(currentTp)

    while(!body.isEmpty) {
      // get vars of current tp
      val vars = varsOf(currentTp)
      println("Vars: " + vars)
      // pick next tp
      vars.foreach{v =>
        val nextTp = findNextTriplePattern(body, v)

        if(nextTp.isDefined) {
          println("Next TP:" + nextTp)
          body.remove(nextTp.get)
        }
      }
    }


    val bodyGraph = RuleUtils.graphOfBody(rule)
    println("Body graph:" + bodyGraph)

    val headGraph = RuleUtils.graphOfHead(rule)

    val headNodes = headGraph.nodes.toList

    headNodes.foreach{node =>
      if(node.value.isVariable) {
        val bodyGraphNode = bodyGraph find node

        bodyGraphNode match {
          case Some(n) =>
            val successor = n findSuccessor (_.outDegree > 0)

            println("Node: " + n)
            println("Out:" + n.outerEdgeTraverser.withDirection(Successors).toList)
            println("In:" + n.outerEdgeTraverser.withDirection(Predecessors).toList)
          case None => println("Not in body")
        }

      }
    }

  }

  def findNextTriplePattern(triplePatterns: mutable.SortedSet[TriplePattern], variable: Node): Option[TriplePattern] = {

    triplePatterns.foreach(tp => {
      tp.getPredicate.equals(variable)
    })
    val candidates = triplePatterns.filter(tp =>
        tp.getSubject.equals(variable) ||
        tp.getPredicate.equals(variable) ||
        tp.getObject.equals(variable))

    if(candidates.isEmpty) {
      None
    } else {
      Option(candidates.head)
    }
  }

  def varsOf(tp: TriplePattern): List[Node] = {
    var vars = List[Node]()

    if(tp.getSubject.isVariable) {
      vars  = vars :+ tp.getSubject
    }

    if(tp.getPredicate.isVariable) {
      vars  = vars :+ tp.getPredicate
    }

    if(tp.getObject.isVariable) {
      vars  = vars :+ tp.getObject
    }

    vars
  }

  def toMultimap(triples: RDD[RDFTriple]) = {

  }

  def main(args: Array[String]) {
    val rules = RuleUtils.load("rules/rdfs-simple.rules")

    val rule = RuleUtils.byName(rules, "rdfs2").get

    rewrite(rule)
  }



}
