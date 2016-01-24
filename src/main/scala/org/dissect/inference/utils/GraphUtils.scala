package org.dissect.inference.utils

import java.io.FileWriter
import javax.xml.transform.TransformerConfigurationException

import org.apache.jena.reasoner.rulesys.Rule
import org.jgrapht.DirectedGraph
import org.jgrapht.ext._
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}

import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge

/**
  * @author Lorenz Buehmann
  *         created on 1/23/16
  */
object GraphUtils {

  implicit class ClassExporter(val graph: scalax.collection.mutable.Graph[Rule, DiEdge]) {
    /**
      * Export the rule dependency graph to GraphML format.
      * @param filename the target file
      */
    def export(filename: String) = {

      val g: DirectedGraph[Rule, DefaultEdge] = new DefaultDirectedGraph[Rule, DefaultEdge](classOf[DefaultEdge])

      val edges = graph.edges.toList

      edges.foreach { e =>
        val nodes = e.nodes.toList
        val source = nodes(0)
        val target = nodes(1)
        g.addVertex(source)
        g.addVertex(target)
        g.addEdge(source, target, new DefaultEdge())
      }

      // In order to be able to export edge and node labels and IDs,
      // we must implement providers for them
      val vertexIDProvider = new VertexNameProvider[Rule]() {
        override def getVertexName(v: Rule): String = v.getName
      }

      val vertexNameProvider = new VertexNameProvider[Rule]() {
        override def getVertexName(v: Rule): String = v.getName
      }

      val edgeIDProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(edge: LabeledEdge): String = {
          g.getEdgeSource(edge) + " > " + g.getEdgeTarget(edge)
        }
      }

      val edgeLabelProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(e: LabeledEdge): String = e.label
      }

//      val exporter = new GraphMLExporter[String,LabeledEdge](
//        vertexIDProvider, vertexNameProvider, edgeIDProvider,edgeLabelProvider)

      val exporter = new GraphMLExporter[Rule, DefaultEdge](
        new IntegerNameProvider[Rule],
        vertexNameProvider,
        new IntegerEdgeNameProvider[DefaultEdge],
        null)

      val fw = new FileWriter(filename)

      exporter.export(fw, g)
    }
  }

  class LabeledEdge(val label: String) extends DefaultEdge {
  }

}
