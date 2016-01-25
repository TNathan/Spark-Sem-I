package org.dissect.inference.utils

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.dissect.inference.rules.RuleEntailmentType
import org.dissect.inference.rules.RuleEntailmentType._

import scala.collection.JavaConversions._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._
import scalax.collection.edge.Implicits._

/**
  * Utility class for rules.
  *
  * @author Lorenz Buehmann
  */
object RuleUtils {


  /**
    * Checks whether a rule is terminological.
    * <p>
    * An rule is considered as terminological, if and only if it contains
    * only terminological triples in its conclusion.
    * </p>
    * @see org.dissect.inference.utils.TripleUtils#isTerminological
    *
    * @param rule the rule to check
    */
  def isTerminological(rule: Rule) : Boolean = {
    var ret = true

    // check whether there are only terminological triples in head
    rule.getHead
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isTerminological(tp.asTriple())) {ret = false}
      )
    ret
  }

  /**
    * Checks whether a rule is assertional.
    * <p>
    * An rule is considered as assertional, if and only if it contains
    * only assertional triples in its premises and conclusion.
    * </p>
    * @see org.dissect.inference.utils.TripleUtils#isAssertional
    *
    * @param rule the rule to check
    */
  def isAssertional(rule: Rule) : Boolean = {
    var ret = true

    // check whether there are only assertional triples in body
    rule.getBody
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isAssertional(tp.asTriple())) {ret = false}
      )

    // check whether there are only assertional triples in head
    rule.getHead
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isAssertional(tp.asTriple())) {ret = false}
      )
    ret
  }

  /**
    * Checks whether a rule is assertional.
    * <p>
    * An rule is considered as hybrid, if and only if it contains both assertional and terminological triples in its
    * premises and only assertional triples in its conclusion.
    * </p>
    * @see org.dissect.inference.utils.TripleUtils#isAssertional
    *
    * @param rule the rule to check
    */
  def isHybrid(rule: Rule) : Boolean = {
    // check for assertional triple in body
    var assertional = false
    rule.getBody
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(TripleUtils.isAssertional(tp.asTriple())) {assertional = true}
      )

    // check for terminological triple in body
    var terminological = false
    rule.getBody
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(TripleUtils.isTerminological(tp.asTriple())) {terminological = true}
      )

    val hybridBody = assertional && terminological

    // we stop if body is not hybrid
    if(!hybridBody) {
      return false
    }

    // check if there are only assertional triples in head
    var assertionalHead = true
    rule.getHead
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isAssertional(tp.asTriple())) {assertionalHead = false}
      )

    assertionalHead
  }

  /**
    * Returns the type of entailment for the given rule
    * @param rule the rule to analyze
    * @return the entailment type
    */
  def entailmentType(rule: Rule) : RuleEntailmentType = {
    if(isAssertional(rule))
      RuleEntailmentType.ASSERTIONAL
    else if(isTerminological(rule))
      RuleEntailmentType.TERMINOLOGICAL
    else if(isHybrid(rule))
      RuleEntailmentType.HYBRID
    else
      None.get
  }

  /**
    * Returns a graph representation of the triple patterns contained in the body of the rule.
    * @param rule the rule
    * @return the directed labeled graph
    */
  def graphOfBody(rule: Rule) : Graph[Node, LDiEdge] = {
    // create empty graph
    val g = Graph[Node, LDiEdge]()

    // add labeled edge p(s,o) for each triple pattern (s p o) in the body of the rule
    rule.getBody.collect { case b: TriplePattern => b }.foreach(
      tp => g += (tp.getSubject ~+> tp.getObject)(tp.getPredicate)
    )
    g
  }

  /**
    * Returns a graph representation of the triple patterns contained in the head of the rule.
    * @param rule the rule
    * @return the directed labeled graph
    */
  def graphOfHead(rule: Rule) : Graph[Node, LDiEdge] = {
    // create empty graph
    val g = Graph[Node, LDiEdge]()

    // add labeled edge p(s,o) for each triple pattern (s p o) in the head of the rule
    rule.getHead.collect { case b: TriplePattern => b }.foreach(
      tp => g += (tp.getSubject ~+> tp.getObject)(tp.getPredicate)
    )
    g
  }

  /**
    * Returns a graph representation of the triple patterns contained in the rule.
    * @param rule the rule
    * @return the directed labeled graph
    */
  def asGraph(rule: Rule) : Graph[Node, LDiEdge] = {
    // create graph for body
    val bodyGraph = graphOfBody(rule)

    // create graph for head
    val headGraph = graphOfHead(rule)

    // return union
    bodyGraph union headGraph
  }

  /**
    * Checks whether a rule itself is cyclic. Intuitively, this means to check for triples produced in the conclusion
    * that are used as input in the premise.
    *
    * This is rather tricky, i.e. a naive approach which simply looks e.g. for predicates that occur in both, premise and conclusion
    * is not enough because, e.g. a rule [(?s ?p =o) -> (?o ?p ?s)] would lead to an infinite loop without producing anything new
    * after one iteration. On the other hand, for rules like [(?s ?p ?o1), (?o1 ?p ?o2) -> (?s ?p ?o2)] it's valid.
    * TODO we do not only have to check for common predicates, but also have to analyze the subjects/objects of the
    * triple patterns.
    *
    * @param rule the rule to check
    * @return whether it's cyclic or not
    */
  def isCyclic(rule: Rule) : Boolean = {
    val ruleType = entailmentType(rule)

    val bodyPredicates = rule.getBody
      .collect { case b: TriplePattern => b }
      .map(tp => tp.getPredicate).toSet
    val headPredicates = rule.getHead
      .collect { case b: TriplePattern => b }
      .map(tp => tp.getPredicate).toSet

    val intersection = bodyPredicates.intersect(headPredicates)
    println(headPredicates)
    println(bodyPredicates)
    println(rule)
    println(intersection)

    ruleType match {
      case TERMINOLOGICAL =>
        // check if there is at least one predicate that occurs in body and head
        val bodyPredicates = rule.getBody
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        val headPredicates = rule.getHead
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet

        bodyPredicates.intersect(headPredicates).nonEmpty
      case ASSERTIONAL =>
        // check if there is at least one predicate that occurs in body and head
        val bodyPredicates = rule.getBody
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        val headPredicates = rule.getHead
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        bodyPredicates.intersect(headPredicates).nonEmpty
      case _ =>
        // check if there is at least one predicate that occurs in body and head
        val bodyPredicates = rule.getBody
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        val headPredicates = rule.getHead
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        bodyPredicates.intersect(headPredicates).nonEmpty

    }
  }

  /**
    * Load a set of rules from the given file.
    * @param filename the file
    * @return a set of rules
    */
  def load(filename: String): Seq[Rule] = {
    Rule.parseRules(org.apache.jena.reasoner.rulesys.Util.loadRuleParserFromResourceFile(filename)).toSeq
  }

  /**
    * Returns a rule by the given name from a set of rules.
    * @param rules the set of rules
    * @param name the name of the rule
    * @return the rule if exist
    */
  def byName(rules: Seq[Rule], name: String): Option[Rule] = {
    rules.foreach(
      r => if (r.getName.equals(name)) return Some(r)
    )
    None
  }


}
