package org.dissect.inference.forwardchaining

import java.util.Collections

import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.sparql.core.BasicPattern
import org.apache.jena.sparql.syntax.{ElementGroup, PatternVars}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFTriple}
import org.dissect.inference.utils.{GraphUtils, RuleUtils, TriplePatternOrdering}
import org.dissect.inference.utils.RuleUtils._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.{existentials, implicitConversions}
import scala.collection.JavaConversions._

/**
  * A naive implementation of the forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerNaive(sc: SparkContext, rules: Set[Rule]) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: RDFGraph) : RDFGraph = {

    val triplesRDD = graph.triples

    rules.foreach{rule =>
      println(rule)
      applyRule(rule, graph)
    }

    graph
  }

  /**
    * Apply a single rule on the given graph.
    *
    * @param rule the rule
    * @param graph the graph
    */
  def applyRule(rule: Rule, graph: RDFGraph) : Unit = {

    val body = collection.mutable.SortedSet[TriplePattern]()(new TriplePatternOrdering()) ++ rule.bodyTriplePatterns.toSet

    // take first triple pattern
    val currentTp = body.head

    while(!body.isEmpty) {
      // get vars of current tp
      val vars = varsOf(currentTp)
      // pick next tp
      vars.foreach(v => findNextTriplePattern(body, v.toString))
    }

    body.foreach(tp =>
      graph.find(tp.asTriple())
    )


    val bodyGraph = RuleUtils.graphOfBody(rule)

    val headGraph = RuleUtils.graphOfHead(rule)

    val headNodes = headGraph.nodes.toList

    headNodes.foreach{node =>
      if(node.value.isVariable) {
        val bodyGraphNode = bodyGraph find node

        bodyGraphNode match {
          case Some(n) =>
            val successor = n findSuccessor (_.outDegree > 0)
            val traverser = n.outerNodeTraverser

            println(traverser.toList)
          case None => println("Not in body")
        }

      }
    }

    println(bodyGraph.edges.toTraversable.toList)

  }

  def findNextTriplePattern(triplePatterns: mutable.SortedSet[TriplePattern], variable: String): Option[TriplePattern] = {
    val candidates = triplePatterns.filter(tp => tp.getSubject.toString == variable || tp.getPredicate.toString == variable || tp.getObject.toString == variable)

    if(candidates.isEmpty) {
      None
    } else {
      Option(candidates.head)
    }
  }

  def varsOf(triple: TriplePattern) = {
    val eg = new ElementGroup()
    eg.addTriplePattern(triple.asTriple())
    PatternVars.vars(eg).toList
  }

  def toMultimap(triples: RDD[RDFTriple]) = {

  }



}
