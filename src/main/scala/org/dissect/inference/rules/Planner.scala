package org.dissect.inference.rules

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.sparql.syntax.{ElementGroup, PatternVars}
import org.apache.spark.rdd.RDD
import org.dissect.inference.data.RDFTriple
import org.dissect.inference.utils.RuleUtils._
import org.dissect.inference.utils.{RuleUtils, TriplePatternOrdering}
import org.apache.jena.graph.Triple

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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

    val body = rule.bodyTriplePatterns.map(tp => tp.asTriple()).toSet

    val visited = mutable.Set[org.apache.jena.graph.Triple]()

//    process(body.head, body, visited)

    // group triple patterns by var
    val map = new mutable.HashMap[Node, collection.mutable.Set[org.apache.jena.graph.Triple]] () with mutable.MultiMap[Node, org.apache.jena.graph.Triple]
    body.foreach{tp =>
      println("TP:" + tp)
      val vars = RuleUtils.varsOf(tp)
      vars.foreach{v =>
        map.addBinding(v,tp)
        println(v + "->" + tp)
      }
    }

    val joins = new mutable.HashSet[Join]

    map.foreach{e =>
      val v = e._1
      val tps = e._2.toList.combinations(2).foreach(c =>
        joins.add(new Join(c(0), c(1), v))
      )
    }

    new Plan(body, rule.headTriplePatterns().toList.head.asTriple(), joins)


//    val bodyGraph = RuleUtils.graphOfBody(rule)
//    println("Body graph:" + bodyGraph)
//
//    val headGraph = RuleUtils.graphOfHead(rule)
//
//    val headNodes = headGraph.nodes.toList
//
//    headNodes.foreach{node =>
//      if(node.value.isVariable) {
//        val bodyGraphNode = bodyGraph find node
//
//        bodyGraphNode match {
//          case Some(n) =>
//            val successor = n findSuccessor (_.outDegree > 0)
//
//            println("Node: " + n)
//            println("Out:" + n.outerEdgeTraverser.withDirection(Successors).toList)
//            println("In:" + n.outerEdgeTraverser.withDirection(Predecessors).toList)
//          case None => println("Not in body")
//        }
//
//      }
//    }

  }

  case class Plan(triplePatterns: Set[Triple], target: Triple, joins: mutable.Set[Join]) {

    def generateJoins() = {

    }

    def addTriplePattern(tp: TriplePattern) = {

    }


  }

  case class Join(tp1: org.apache.jena.graph.Triple, tp2: org.apache.jena.graph.Triple, joinVar: Node) {
    override def toString() = tp1.toString + " JOIN " + tp2.toString + " ON " + joinVar
  }

  def process(tp: org.apache.jena.graph.Triple, body: mutable.ListBuffer[org.apache.jena.graph.Triple], visited: mutable.Set[org.apache.jena.graph.Triple]): Unit = {
    println("TP:" + tp)
    visited += tp

    // get vars of current triple pattern
    val vars = varsOf(tp)
    println("Vars: " + vars)

    // pick next connected triple pattern
    vars.foreach{v =>
      val nextTp = findNextTriplePattern(body, v)

      if(nextTp.isDefined) {
        val tp2 = nextTp.get
        println("Next TP:" + tp2)
        println(new Join(tp, tp2, v))

        if(!visited.contains(tp2)) {
          process(tp2, body, visited)
        }
      }
    }
    body -= tp
  }

  def findNextTriplePattern(triplePatterns: mutable.Seq[org.apache.jena.graph.Triple], variable: Node): Option[org.apache.jena.graph.Triple] = {

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

  def toMultimap(triples: RDD[RDFTriple]) = {

  }
}
