package org.dissect.inference.rules

import java.io.File

import scala.xml.XML

/**
  * @author Lorenz Buehmann
  */
class TestCases {

  def loadTestCases(directory: File) = {

    directory.listFiles().filter(f => f.isDirectory).foreach { subDirectory =>

      val metadataFile = subDirectory.listFiles().filter(_.getName.endsWith(".metadata.properties")).toList.head

      val metadata = XML.loadFile(metadataFile)

      val isEntailmentTestCase = (metadata \\ "entry").filter(n => n.attribute("key").get.text == "testcase.type").text == "POSITIVE_ENTAILMENT"

    }
  }
}
