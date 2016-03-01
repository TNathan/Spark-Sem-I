package org.dissect.inference.forwardchaining

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.RDFGraph
import org.dissect.inference.utils.RuleUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.{existentials, implicitConversions}

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

    val bodyGraph = RuleUtils.graphOfBody(rule)

    val headGraph = RuleUtils.graphOfHead(rule)

    val headNodes = headGraph.nodes.toList

    headNodes.foreach{node =>
      if(node.value.isVariable) {
        val bodyGraphNode = bodyGraph find node

        bodyGraphNode match {
          case Some(n) => {
            val successor = n findSuccessor (_.outDegree > 0)
            val traverser = n.outerNodeTraverser

            println(traverser.toList)
          }
          case None => println("Not in body")
        }

      }
    }

    println(bodyGraph.edges.toTraversable.toList)

  }



}
