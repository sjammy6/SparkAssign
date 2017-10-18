import java.nio.file.attribute.FileAttribute
import java.nio.file.{FileAlreadyExistsException, StandardOpenOption, Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.io.Source

import scala.math.Ordering

object GraphX {
  

  var inputFile = "C:\\dataset\\freebase-wex-2009-01-12-articles.tsv"
  var masterUrl = "local"
  var findTop100Universities = false;
  val topCount = 100
  val iterations = 5

  val graphxLogFile = "graphx.log"
  val universityLogFile = "top100Universities.log"
  
  def main(args: Array[String]) {
 
    //arg1 will be absolute path of input FILE
    if (args.length >= 1) {
      inputFile = args(0).toString.trim
    }  
    //arg2 will be the master spark node location.
    if (args.length >= 2) {
      masterUrl = args(1).toString.trim
    }
    //arg3 is used to whether to calculate university list or not
    if (args.length >= 3) {
      findTop100Universities = args(2).toString.trim
    }
    
    //Create a configurationfile
    val config = new config().setAppName("RankPagesUsingGraphX")
    val sc = new SparkContext(config)

   
    val wiki: RDD[String] = sc.textFile(inputFile, 1)
    Files.deleteIfExists(Paths.get(path))

    var list: Graph[(Double, String), Double] = null

    list = printTopPages(wiki, masterUrl, topC, iterations, graphxLogFile, sc)

    if(findTop100Universities){
      var startTime = System.currentTimeMillis()
         Files.deleteIfExists(Paths.get(universityLogFile))
      outputToFile("==========starting task 3, calculating top 100 universities========", universityLogFile )
      val l = Source.fromURL(getClass.getResource("/rawUniversitiesFile.txt")).getLines().toList
      var p = list.vertices.top(list.triplets.count().toInt) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
      }.filter { x =>
        l contains (x._2._2)
      }.take(100).foreach(x => outputToFile("\n" + x._2._2 + " ===converged rank===> " + x._2._1, universityLogFile))
      outputToFile("Time to complete the task 3 : " + (System.currentTimeMillis() - startTime ), universityLogFile )
      outputToFile("=============Ending task3==================", universityLogFile )
    }
    
  }
  def printTopPages(wikiData: String, masterUrl: String, topC: Int, iters: Int, path: String, sc: SparkContext): Graph[(Double, String), Double] = {
   
    //Delete older log file if exists
    Files.deleteIfExists(Paths.get(path))
    //Store time for time analysis
    var startTime = System.currentTimeMillis()
    //put input data into RDD
    val wiki: RDD[String] = sc.textFile(wikiData).coalesce(20)

    //Define the Page class
    case class Page(id: Long, title: String, xml: String)

    var r: String = ""
    val Pages = wiki.map(_.split('\t')).
      filter(line => line.length > 1).
      map(line => new Page(line(0).trim.toLong, line(1).trim, line(3).trim)).cache()

    outputToFile("Number of input articles : " + Pages.count(), path)

    val vertices: RDD[(VertexId, String)] = Pages.map(a => (pageHash(a.title), a.title))

    outputToFile("Number of vertices : " + vertices.count(), path)

    val pattern = "<target>.+?<\\/target>".r

    val edges: RDD[Edge[Double]] = Pages.flatMap { a =>
      val srcId = pageHash(a.title)
      pattern.findAllIn(a.xml).map { link =>
        val dstId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        Edge(srcId, dstId, 1.0)
      }
    }
    outputToFile("Number of Edges : " + edges.count(), path)

    //Building the graph
    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty }).cache
    //Writing metrics to file
    outputTofile("------------------Graph Properties---------",path)
    outputTofile("Number of Vertices in the graph : " + graph.vertices.count(),path)
    outputTofile("Number of Edges in the graph : " + graph.edges.count(),path)
    outputTofile("Number of Triplets in the graph : " + graph.triplets.count(),path)
    outputToFile("Time taken to build graph : " + (System.currentTimeMillis() - startTime), path)

    startTime = System.currentTimeMillis()

    val prGraph: Graph[Double, Double] = graph.staticPageRank(iters).cache

    val titleAndPrGraph: Graph[(Double, String), Double] = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
    
    outputToFile("==============Top100 using graphX===============:", path)
    var p = titleAndPrGraph.vertices.top(titleAndPrGraph.triplets.count().toInt) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.take(topC).foreach(t => {
      outputToFile(t._2._2 + " ====converged rank====> " + t._2._1, path)
    })
    outputToFile("Time to complete the task : " + (System.currentTimeMillis() - startTime ), path)
    titleAndPrGraph
  }

  def outputToFile(r: String, path: String) = {
    print(r + "\n")
    Files.outputToFile(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

  def pageHash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

}