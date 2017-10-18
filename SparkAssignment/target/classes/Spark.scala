import java.nio.file.{StandardOpenOption, Paths, Files}

import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Scanner


import scala.io.Source

import scala.math.Ordering
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark {
  
  var wikiData = "C:\\dataset\\freebase-wex-2009-01-12-articles.tsv"
  var masterUrl = "local"
  val topC = 100
  val iters = 5
  val sparkLogFile = "spark.log"
 
  
  def main(args: Array[String]) {
 
   //arg1 will be absolute path of input FILE 
    if (args.length >= 1) {
      wikiData = args(0).toString.trim
    }
    
    //arg2 will be the master spark node location.
    if (args.length >= 2) {
      masterUrl = args(1).toString.trim
    }

    //Create a configurationfile
    val sparkConf = new SparkConf().setAppName("PageRankUsingSpark")
    val sc = new SparkContext(sparkConf)

    var startTime = System.currentTimeMillis()
    val wiki: RDD[String] = sc.textFile(wikiData, 1)
    Files.deleteIfExists(Paths.get(sparkresult))
    write("=================Starting task 1========================", sparkLogFile)

    //Define the article and link between articles classes.
    case class Article(val id: Long, val title: String, val xml: String)

    val articles = wiki.map(_.split('\t')).
      filter(line => line.length > 1).
      map(line => new Article(line(0).trim.toLong, line(1).trim, line(3).trim)).cache()

    write("Total articles : " + articles.count(), sparkLogFile)

    val pattern = "<target>.+?<\\/target>".r

    val links: RDD[(String, Iterable[String])] = articles.flatMap { a =>

      pattern.findAllIn(a.xml).map { link =>
        val dstId = link.replace("<target>", "").replace("</target>", "")
        (a.title, dstId)
      }
    }.distinct().groupByKey().cache()

    write("Total Links Iters : " + links.count(), sparkLogFile)

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) { 
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }   
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }


    var output = ranks.sortBy(_._2, false).collect()

    output.take(topC).foreach(tup => {
      write(tup._1 + " ====converged rank====> " + tup._2, sparkLogFile)
    })
    write("Total time taken to complete task 1: " + (System.currentTimeMillis() - startTime ), sparkLogFile)
  }

  def write(r: String, path: String) = {
    print(r + "\n")
    Files.write(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
  
}