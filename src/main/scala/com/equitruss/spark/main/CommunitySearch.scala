package com.equitruss.spark.main

//   1 1 output_search output_test/superEdges/part-00000 3 output_test/VerticesWithNodes/part-00000 output_test/superNodesWithVertices/part-00000 output_test/superNodes/part-00000


import scala.io.Source
import breeze.linalg.min
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.reflect.ClassTag
import scala.math._
import scala.reflect.ClassTag
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql._
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import Equitruss_Scala.{Vert, VertexSet, getCoUn}

object CommunitySearch {

  val initialMsg="-10"

  type VertexTruss = Int

  case class Vert(u:Long)

  case class EdgeVert(name:String,count:Int)

  case class NewEdge(edge:EdgeVert,tablet:Map[String,Map[String,Int]])

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Load graph").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //  import session.sqlContext.implicits._
    import sqlContext.implicits._
    CommunitySearch(args, sc)
    sc.stop()
  }



  def CommunitySearch(args : Array[String], sc : SparkContext){
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val maxIter = args(1).toInt
    val partitions = args(0).toInt
    val Kmin = args(4).toInt
    val direction: EdgeDirection = EdgeDirection.Either  // In and Out
    val verticesWithNodes = sc.textFile( args(5)).map(x=>{((x.split(", ")(0).stripPrefix("(")),(x.split(", ")(1)).stripSuffix(")"))}).toDF("vertex","supernodes")
    var participatingSuperNodes = ArrayBuffer[String]()
    val queryOutput =(verticesWithNodes.select("supernodes") where (($"vertex" === args(8)))).collect()
    queryOutput.head.toString().stripSuffix("]").stripPrefix("[").split(" ").foreach(k=>{
      val w=k.trim.stripPrefix("(").stripSuffix(")").trim; if(  w.split(",").size==2   )
      {if(w.split(",")(1).toInt>=Kmin)
      {participatingSuperNodes +=k}}})

    val startTimeMillis = System.currentTimeMillis()
    val graph= GraphLoader.edgeListFile(sc,args(3), true,partitions,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).removeSelfEdges().groupEdges((e1, e2) => e1)
    val lines = sc.textFile(args(7))
    val superNodes : RDD[(VertexId,(Int ))]  = lines.map(k=>{ (k.split(" ")(0).toInt, (k.split(" ")(1).toInt   )) })
    val superNodesFiltered = VertexRDD(superNodes)
    val modifiedGraph=  graph.outerJoinVertices(superNodesFiltered){
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    val mEdges=modifiedGraph.edges  //edge triplets containing the srcAttr, dstAttr and common neighbors
    val edgeCount=mEdges.count()    //number of edges
    val vertCount=modifiedGraph.vertices.count()     //number of vertices
    val vrdd : RDD[(VertexId,(Int ,Int,Double,Double,ArrayBuffer[String]))] = modifiedGraph.vertices.map(ver=>{
      if(participatingSuperNodes.contains(ver.toString)) {
        (ver._1,((ver._2.toString.toInt),(Kmin),(Double.PositiveInfinity),(0.0),(participatingSuperNodes)))}

      else{
        (ver._1,((ver._2.toString.toInt),(Kmin),(0.0),(0.0),(participatingSuperNodes)))}
    })


    val mverts = VertexRDD(vrdd)

    val lineGraph= Graph(mverts.repartition(partitions), mEdges.repartition(partitions), (  0 , Kmin, Double.PositiveInfinity, Double.PositiveInfinity,participatingSuperNodes) , StorageLevel.MEMORY_AND_DISK ,StorageLevel.MEMORY_AND_DISK).removeSelfEdges()

    val bfsGraph = lineGraph.pregel(initialMsg,maxIter,EdgeDirection.Both)(UpdateForSearch,sendMsgForSearch,mergeMsgForSearch)
    val endTimeMillis = System.currentTimeMillis()
    println("Execution time "+(endTimeMillis-startTimeMillis)/1000)
    val communities = bfsGraph.vertices.map(l=>{l._2._5}).distinct()
    communities.map(k=>{k}).repartition(partitions).saveAsTextFile(args(2)+"/searchResult")

  }

  /*
currentTruss
minTruss
ToVisit double.PositiveInfinity for toVisit, 0.0 otherwise
Visited double.PositiveInfinity for visited, 0.0 otherwise
RelevantSuperNodes
*/

  def sendMsgForSearch(triplet: EdgeTriplet[( Int , Int, Double ,Double,ArrayBuffer[String]),Int]): Iterator[(VertexId, String)] = {

    val aa = triplet.srcAttr
    val bb = triplet.dstAttr

    if(bb._1 >= bb._2 && aa._4==0.0 && aa._3==Double.PositiveInfinity) {     // Current trussness > min trussmess(kmin) 
      return Iterator((triplet.dstId,"toVisit"),((triplet.srcId,"Visited")))
    }
    else {

      if (aa._4==0.0) {
        return Iterator(((triplet.srcId, "Visited")))
      }
      else {
        return Iterator.empty
      }
    }
  }


  def mergeMsgForSearch(msg1: String, msg2:String): String = msg1+":"+msg2 // check message

  def UpdateForSearch(vertexID : VertexId,value: (Int,Int,Double,Double,ArrayBuffer[String]), message: String): (Int,Int, Double,Double,ArrayBuffer[String]) = {
    if (message == initialMsg){

      return ( value._1,value._2,value._3,value._4,value._5)
    }else
    {
      val msg= message.split(":")                                                 // split message by ":"
      if(msg.contains("Visited")){
        if(value._1>=value._2){
          return ( value._1,value._2,value._3,Double.PositiveInfinity,value._5)
        }
      }
      else{
        if(msg.contains("toVisit")){   //Visits the Supernode and sends messages to its neighbors
          if(value._3 == 0.0){
            value._5+="("+vertexID.toString+","+value._1.toString+")"
            return ( value._1,value._2,Double.PositiveInfinity,value._4,value._5)
          }
        }
      }
    }
    return ( value._1,value._2,value._3,value._4,value._5)
  }
}

