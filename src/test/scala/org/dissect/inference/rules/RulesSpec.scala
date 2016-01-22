package org.dissect.inference.rules

import org.dissect.inference.utils.RuleUtils
import org.scalatest.FlatSpec

/**
  * Test class for rules.
  *
  * @author Lorenz Buehmann
  */
class RulesSpec extends FlatSpec {

  val rules = RuleUtils.load("test.rules")

  "rule 'prp-trp'" should "be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "prp-trp").get) == true)
  }

  "rule 'prp-symp'" should "not be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "prp-symp").get) == false)
  }

  "rule 'rdfs11'" should "be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "rdfs11").get) == true)
  }

  "rule 'rdfs2'" should "not be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "rdfs2").get) == false)
  }

}
