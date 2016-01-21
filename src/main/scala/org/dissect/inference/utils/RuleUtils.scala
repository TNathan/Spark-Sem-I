package org.dissect.inference.utils

import org.apache.jena.graph.Triple
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.dissect.inference.rules.RuleEntailmentType
import org.dissect.inference.rules.RuleEntailmentType._

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
    * Checks whether a rule itself is cyclic, i.e. it produces triples in the conclusion
    * that are used as input in the premise.
    * @param rule the rule to check
    * @return whether it's cyclic or not
    */
  def isCyclic(rule: Rule) : Boolean = {
    if(isTerminological(rule)) {
      // check if there is at least one predicate that occurs in body and head
      val bodyPredicates = rule.getBody
        .collect{case b:TriplePattern => b}
        .map(tp => tp.getPredicate).toSet
      val headPredicates = rule.getBody
        .collect{case b:TriplePattern => b}
        .map(tp => tp.getPredicate).toSet

      bodyPredicates.intersect(headPredicates).isEmpty
    } else {
      true
    }
  }

}
