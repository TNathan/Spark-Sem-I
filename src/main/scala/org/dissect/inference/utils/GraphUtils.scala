package org.dissect.inference.utils

import java.io.{ByteArrayOutputStream, File, FileOutputStream, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.itextpdf.text.PageSize
import org.apache.jena.graph.Node
import org.apache.jena.reasoner.rulesys.Rule
import org.gephi.graph.api.GraphController
import org.gephi.io.exporter.api.ExportController
import org.gephi.io.exporter.preview.PDFExporter
import org.gephi.io.importer.api.{EdgeDirectionDefault, ImportController}
import org.gephi.io.processor.plugin.DefaultProcessor
import org.gephi.layout.plugin.force.StepDisplacement
import org.gephi.layout.plugin.force.yifanHu.YifanHuLayout
import org.gephi.preview.api.{Item, PreviewController, PreviewProperty}
import org.gephi.preview.types.EdgeColor
import org.gephi.project.api.ProjectController
import org.jgrapht.DirectedGraph
import org.jgrapht.ext._
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}
import org.openide.util.Lookup

import scala.collection.JavaConversions._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.LDiEdge

/**
  * @author Lorenz Buehmann
  *         created on 1/23/16
  */
object GraphUtils {

  implicit class ClassRuleDependencyGraphExporter(val graph: scalax.collection.mutable.Graph[Rule, DiEdge]) {
    /**
      * Export the rule dependency graph to GraphML format.
      *
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

  implicit class ClassRuleTriplePatternGraphExporter(val graph: scalax.collection.mutable.Graph[Node, LDiEdge]) {

    def export2(filename: String) = {
      //Init a project - and therefore a workspace
      val pc = Lookup.getDefault().lookup(classOf[ProjectController]);
      pc.newProject();
      val workspace = pc.getCurrentWorkspace();

      //See if graph is well imported
      val graphModel = Lookup.getDefault().lookup(classOf[GraphController]).getGraphModel;
      val g = graphModel.getDirectedGraph
      val factory = graphModel.factory()

      val edges = graph.edges.toList

      val nodeCache = collection.mutable.Map[String, org.gephi.graph.api.Node]()

      edges.foreach { e =>
        val nodes = e.nodes.toList
        var source = nodeCache.get(nodes(0).toString()) match {
          case Some(value) => value
          case None => {
            val source2 = factory.newNode(nodes(0).toString())
            source2.setLabel(nodes(0).toString())
            println()
            g.addNode(source2)
            nodeCache(nodes(0).toString()) = source2
            source2
          }
        }
        var target = nodeCache.get(nodes(1).toString()) match {
          case Some(value) => value
          case None => {
            val target2 = factory.newNode(nodes(1).toString())
            target2.setLabel(nodes(1).toString())
            g.addNode(target2)
            nodeCache(nodes(1).toString()) = target2
            target2
          }
        }
        val edge = factory.newEdge(source, target, true)


        g.addEdge(edge)
      }

      println("Nodes: " + g.getNodeCount());
      println("Edges: " + g.getEdgeCount());

//        val target = factory.newNode(nodes(1).toString())
//        target.setLabel(nodes(1).value.toString)
//        val edge = factory.newEdge(source, target, true)
//        g.addNode(source)
//        g.addNode(target)
//        g.addEdge(edge)
//      }

      //Run YifanHuLayout for 100 passes - The layout always takes the current visible view
      val layout = new YifanHuLayout(null, new StepDisplacement(1f));
      layout.setGraphModel(graphModel);
      layout.resetPropertiesValues();
      layout.setOptimalDistance(200f);

      layout.initAlgo();
      for (i <- 0 to 100 if layout.canAlgo()) {
        layout.goAlgo();
      }
      layout.endAlgo();

      val model = Lookup.getDefault().lookup(classOf[PreviewController]).getModel();
      model.getProperties().putValue(PreviewProperty.SHOW_NODE_LABELS, true);
      model.getProperties().putValue(PreviewProperty.EDGE_CURVED, false);
      model.getProperties().putValue(PreviewProperty.EDGE_COLOR, new EdgeColor(java.awt.Color.GRAY));
      model.getProperties().putValue(PreviewProperty.EDGE_THICKNESS, 0.1f);
      val font = model.getProperties().getFontValue(PreviewProperty.NODE_LABEL_FONT).deriveFont(8)
      model.getProperties().putValue(PreviewProperty.NODE_LABEL_FONT, model.getProperties().getFontValue(PreviewProperty.NODE_LABEL_FONT).deriveFont(8));

      for (item <- model.getItems(Item.NODE_LABEL)) {
        println(item)
      }


      //Export full graph
      val ec = Lookup.getDefault().lookup(classOf[ExportController]);

      ec.exportFile(new File("io_gexf.gexf"));

      //PDF Exporter config and export to Byte array
      val pdfExporter = ec.getExporter("pdf").asInstanceOf[PDFExporter];
      pdfExporter.setPageSize(PageSize.A0);
      pdfExporter.setWorkspace(workspace);
      val baos = new ByteArrayOutputStream();
      ec.exportStream(baos, pdfExporter);
      new FileOutputStream(filename + ".pdf").write(baos.toByteArray())


    }
    /**
      * Export the rule dependency graph to GraphML format.
      *
      * @param filename the target file
      */
    def export(filename: String) = {

      val g: DirectedGraph[Node, LabeledEdge] = new DefaultDirectedGraph[Node, LabeledEdge](classOf[LabeledEdge])

      val edges = graph.edges.toList

      edges.foreach { e =>
        val nodes = e.nodes.toList
        val source = nodes(0)
        val target = nodes(1)
        g.addVertex(source)
        g.addVertex(target)
        g.addEdge(source, target, new LabeledEdge(e.label.toString))
      }

      // In order to be able to export edge and node labels and IDs,
      // we must implement providers for them
      val vertexIDProvider = new VertexNameProvider[Node]() {
        override def getVertexName(v: Node): String = v.getName
      }

      val vertexNameProvider = new VertexNameProvider[Node]() {
        override def getVertexName(v: Node): String = v.getName
      }

      val edgeIDProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(edge: LabeledEdge): String = {
          g.getEdgeSource(edge) + " > " + edge.label + " > " + g.getEdgeTarget(edge)
        }
      }

      val edgeLabelProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(e: LabeledEdge): String = e.label
      }

      //      val exporter = new GraphMLExporter[String,LabeledEdge](
      //        vertexIDProvider, vertexNameProvider, edgeIDProvider,edgeLabelProvider)

      val exporter = new GraphMLExporter[Node, LabeledEdge](
        new IntegerNameProvider[Node],
        vertexNameProvider,
        new IntegerEdgeNameProvider[LabeledEdge],
        edgeLabelProvider)

      val fw = new FileWriter(filename)

      exporter.export(fw, g)

      val path = Paths.get(filename);
      val charset = StandardCharsets.UTF_8;

      var content = new String(Files.readAllBytes(path), charset);
      content = content.replaceAll("vertex_label", "node_label");
      Files.write(path, content.getBytes(charset));

      // Gephi
      //Init a project - and therefore a workspace
      val pc = Lookup.getDefault().lookup(classOf[ProjectController]);
      pc.newProject();
      val workspace = pc.getCurrentWorkspace();

      //Get controllers and models
      val importController = Lookup.getDefault().lookup(classOf[ImportController]);

      //Import file
      val file = new File(filename);
      var container = importController.importFile(file);
      container.getLoader().setEdgeDefault(EdgeDirectionDefault.DIRECTED);   //Force DIRECTED
      //        container.setAllowAutoNode(false);  //Don't create missing nodes

      //Append imported data to GraphAPI
      importController.process(container, new DefaultProcessor(), workspace);

      //See if graph is well imported
      val graphModel = Lookup.getDefault().lookup(classOf[GraphController]).getGraphModel;
      val diGraph = graphModel.getDirectedGraph();
      System.out.println("Nodes: " + diGraph.getNodeCount());
      System.out.println("Edges: " + diGraph.getEdgeCount());

      for(i <- 0 to graphModel.getNodeTable.countColumns()-1) {
        val col = graphModel.getNodeTable.getColumn(i)
        println(col.getId)
        println(diGraph.getNodes().toList(0).getAttribute(col))

      }


      for(node <- diGraph.getNodes.toCollection.toList) {
//        println(node.getAttribute("node_label"))
      }

      //Run YifanHuLayout for 100 passes - The layout always takes the current visible view
      val layout = new YifanHuLayout(null, new StepDisplacement(1f));
      layout.setGraphModel(graphModel);
      layout.resetPropertiesValues();
      layout.setOptimalDistance(200f);

//      layout.initAlgo();
//      for (i <- 0 to 100 if layout.canAlgo()) {
//        layout.goAlgo();
//      }
//      layout.endAlgo();

      val model = Lookup.getDefault().lookup(classOf[PreviewController]).getModel();
      model.getProperties().putValue(PreviewProperty.SHOW_NODE_LABELS, true);
      model.getProperties().putValue(PreviewProperty.EDGE_CURVED, false);
      model.getProperties().putValue(PreviewProperty.EDGE_COLOR, new EdgeColor(java.awt.Color.GRAY));
      model.getProperties().putValue(PreviewProperty.EDGE_THICKNESS, 0.1f);
      model.getProperties().putValue(PreviewProperty.NODE_LABEL_FONT, model.getProperties().getFontValue(PreviewProperty.NODE_LABEL_FONT).deriveFont(8));
//      model.getProperties.putValue(Item.NODE_LABEL, "vertex_label")
      model.getProperties.putValue(Item.NODE_LABEL, "Vertex Label")

      for (item <- model.getItems(Item.NODE_LABEL)) {
        println(item)
      }


  //Export full graph
      val ec = Lookup.getDefault().lookup(classOf[ExportController]);
//      ec.exportFile(new File("io_gexf.gexf"));

      //PDF Exporter config and export to Byte array
      val pdfExporter = ec.getExporter("pdf").asInstanceOf[PDFExporter];
      pdfExporter.setPageSize(PageSize.A0);
      pdfExporter.setWorkspace(workspace);
      val baos = new ByteArrayOutputStream();
      ec.exportStream(baos, pdfExporter);
      new FileOutputStream(filename + ".pdf").write(baos.toByteArray())
    }
  }

  class LabeledEdge(val label: String) extends DefaultEdge {
  }

}
