package com.equitruss.spark.main

import breeze.linalg.min
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.math._
import scala.reflect.ClassTag
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql._
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._





//1 1 output_Amazon0302 Amazon0302.txt




object Equitruss_Scala {
  val initialMsg="-10"

  type VertexSet = OpenHashSet[VertexId]

  case class Vert(u:Long,v:Long)

  case class EdgeVert(name:String,count:Int)

  case class NewEdge(edge:EdgeVert,tablet:Map[String,Map[String,Int]])

  def main(args: Array[String]) {
    //    val session = SparkSession.builder().appName("EquiTruss").config("spark.master", "local").getOrCreate()

    val conf = new SparkConf().setAppName("Equitruss")//.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //  import session.sqlContext.implicits._
    import sqlContext.implicits._
    CreateIndex(args, sc)
    sc.stop()
  }



  def CreateIndex(args : Array[String], sc : SparkContext){
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val maxIter = args(1).toInt
    val partitions = args(0).toInt
    val direction: EdgeDirection = EdgeDirection.Either  // In and Out
    val startTimeMillis = System.currentTimeMillis()
    val graph= GraphLoader.edgeListFile(sc,args(3), true,partitions,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).removeSelfEdges().groupEdges((e1, e2) => e1)
    val modifiedGraph= getTriangle(graph)
    val eMap:Map[Long,Map[Long,Int]]=Map()
    val mEdges=modifiedGraph.edges  //edge triplets containing the srcAttr, dstAttr and common neighbors
    val edgeCount=mEdges.count()    //number of edges
    val vertCount=modifiedGraph.vertices.count()//number of vertices
    val vertLineGraph:VertexRDD[(Vert,Int,Map[Long,Map[Long,Int]],Int,(Long,Int),String)]= VertexRDD(mEdges.map(edge=>{(Vert(edge.srcId.toLong, edge.dstId.toLong) , edge.attr._1+2 , eMap , 100000, (-1L,9999),new String(""))}).zipWithIndex.map(vertex=>{(vertex._2,vertex._1)}))
    //vertex                int                    map   int
    //         ((edge.srcId, edge.dstId), no. of common neighbors or triangles, emap , 100000),edgeID from oringinal graph
    val vertMapping: scala.collection.immutable.Map[Vert,Long]=vertLineGraph.map(vert=>{(vert._2._1 , vert._1)}).collectAsMap.toMap
    val broadCastLookupMap = sc.broadcast(vertMapping)
    val edgeLineGraph = mEdges.flatMap(edge=>{       //iterate through each edge in mEdges
      for(u<-edge.attr._2.iterator) yield            //iterate through the common neighbour set
      {
        if(u<edge.srcId)
        {
          (Vert(edge.srcId.toLong , edge.dstId.toLong), Vert(u.toLong, edge.srcId.toLong), Vert(u.toLong, edge.dstId.toLong))
        }else{
          if(u < edge.dstId)
          {
            (Vert(edge.srcId.toLong , edge.dstId.toLong), Vert(edge.srcId.toLong , u.toLong), Vert(u.toLong , edge.dstId.toLong))
          }else{
            (Vert(edge.srcId.toLong, edge.dstId.toLong) , Vert(edge.srcId.toLong , u.toLong), Vert(edge.dstId.toLong , u.toLong))
          }
        }
      }
    }).flatMap(edge=>{
      List(org.apache.spark.graphx.Edge(broadCastLookupMap.value.get(edge._1).head.toLong,broadCastLookupMap.value.get(edge._2).head.toLong,1L),
        org.apache.spark.graphx.Edge(broadCastLookupMap.value.get(edge._1).head.toLong,broadCastLookupMap.value.get(edge._3).head.toLong,1L),
        org.apache.spark.graphx.Edge(broadCastLookupMap.value.get(edge._2).head.toLong,broadCastLookupMap.value.get(edge._3).head.toLong,1L))
    })
    val lineGraph= Graph(vertLineGraph.repartition(partitions), edgeLineGraph.repartition(partitions), (Vert(0L , 0L) , 2 , eMap , 10000000, (-1L,9999), new String("")) , StorageLevel.MEMORY_AND_DISK ,StorageLevel.MEMORY_AND_DISK).removeSelfEdges().groupEdges((e1, e2) => e1).cache()
    val TrussGraph = lineGraph.pregel(initialMsg,1,EdgeDirection.Both)(vprogTruss,sendMsgTruss,mergeMsgTruss)
    val minGraph = TrussGraph.pregel(initialMsg,1,EdgeDirection.Both)(updateTablet,sendMsgTablet,mergeMsgTruss)
    val ktmax=minGraph.vertices.map(v=>{v._2._2}).max                           //Max truss value
    val filteredEdge=minGraph.vertices.filter(v=>{v._2._2==ktmax})
    val filteredEdgesCount=filteredEdge.count()
    val filteredVertexCount=filteredEdge.flatMap(v=>{List(v._2._1.u,v._2._1.v)}).distinct.count()
    val GraphwithSuperNodes = minGraph.pregel(initialMsg,maxIter,EdgeDirection.Both)(UpdateForSuperNodes,sendMsgTrussForSuperNodes,mergeMsgTruss)
    val GraphwithSuperEdges = GraphwithSuperNodes.pregel(initialMsg,1,EdgeDirection.Both)(UpdateForSuperEdges,sendMsgTrussForSuperEdges,mergeMsgTruss)
    val sedg = new mutable.HashSet[Edge[String]]()
    GraphwithSuperEdges.vertices.map(l =>{ l._2._6}).repartition(1).distinct().collect().foreach(k=>{if(k.toString.split("->").size==2) {
      val src = k.toString.split("->")(0); val iter = k.toString.split("->")(1).split("\\|").iterator;
      while (iter.hasNext) {
        val dest = iter.next().toString
        if (dest != "" ) {
          if (!(sedg.contains(Edge(src.toLong ,  dest.toLong, "super edge"))  || sedg.contains(Edge(dest.toLong ,  src.toLong, "super edge"))))
            sedg += Edge(src.toLong ,  dest.toLong, "super edge")
        }}}})
    val superNodesWithVertices=  GraphwithSuperEdges.vertices.map(k=>{(k._1,((k._2._1.u)),((k._2._1.v)),((k._2._2)),((k._2._5)))})
    val superNodesintermediate1 = superNodesWithVertices.map(k=>(k._5,k._2+"|"+k._3+"|"))
    val superNodesintermediate2 = superNodesintermediate1.reduceByKey((a,b)=>a+b)
    val superNodesintermediate3 = superNodesintermediate2.map(k=>{(k._1,k._2.toString.split("\\|").distinct)})
    superNodesintermediate3.map(u=>{ var k = ""
      val p = u._2.iterator
      while (p.hasNext){k= k+" "+ p.next().toString}
      (u._1,k)}).repartition(partitions).saveAsTextFile(args(2)+"/superNodesWithVertices")

    val superNodesintermediate4 = superNodesWithVertices.map(k=>{ Seq((k._2,k._5.toString()),(k._3,k._5.toString())) }).flatMap(seq => seq).reduceByKey((a,b)=>a+"|"+b+"|")
    val superNodesintermediate5 = superNodesintermediate4.map(s=>{(s._1,s._2.split("\\|").distinct)})
    superNodesintermediate5.map(u=>{ var k = ""
      val p = u._2.iterator
      while (p.hasNext){k= k+" "+ p.next().toString}
      (u._1,k) }).repartition(partitions).saveAsTextFile(args(2)+"/VerticesWithNodes")
    val superNodes :VertexRDD[Int]= VertexRDD( GraphwithSuperNodes.vertices.map(l =>{(l._2._5._1 , l._2._5._2)}).repartition(1))
    val edgerdddd = sedg.toArray
    val superEdges : RDD[Edge[String]] = sc.parallelize(edgerdddd)
    val superGraph = Graph  (superNodes.repartition(partitions),superEdges.repartition(partitions))
    superGraph.vertices.map(v=>{v._1+" "+v._2}).repartition(1).saveAsTextFile(args(2)+"/superNodes")
    superGraph.edges.map(e=>{e.srcId+" "+e.dstId}).repartition(1).saveAsTextFile(args(2)+"/superEdges")
    minGraph.edges.map(e=>{e.srcId+"\t"+e.dstId}).repartition(1).saveAsTextFile(args(2)+"/edgeList")
    GraphwithSuperEdges.vertices.map(v=>{v._1+"\t"+v._2._1.u+"\t"+v._2._1.v+"\t"+v._2._2}).repartition(1).saveAsTextFile(args(2)+"/edgeMapping")
    val endGraphTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endGraphTimeMillis - startTimeMillis) / 1000
    println("Maximal k-Truss Value : "+ktmax)
    println("Total Execution Time : "+durationSeconds.toString() + "s")
    println("Total Edges Initial : "+edgeCount)
    println("Total Vertices Initial :"+vertCount)
    println("Filtered Edge Count :"+filteredEdgesCount)
    println("Filtered Vertex Count :"+filteredVertexCount)
  }


  def sendMsgTrussForSuperEdges(triplet: EdgeTriplet[(Vert, Int , Map[Long,Map[Long,Int]] , Int, (Long, Int), String), Long]): Iterator[(VertexId, String)] = {
    val sourceVertex = triplet.srcAttr
    val destVertex=triplet.dstAttr
    if(sourceVertex._5._1==destVertex._5._1)
    {
      return Iterator.empty
    }else {
      if (sourceVertex._5._1.toLong < destVertex._5._1.toLong) {
        return Iterator((triplet.srcId, sourceVertex._5._1 + "<-->" + destVertex._5._1),(triplet.dstId, sourceVertex._5._1 + "<-->" + destVertex._5._1))
      }
      else {
        return Iterator((triplet.srcId, destVertex._5._1 + "<-->" + sourceVertex._5._1),(triplet.dstId, destVertex._5._1 + "<-->" + sourceVertex._5._1))
      }
    }
  }

  def UpdateForSuperEdges(vertexId: VertexId, value: (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long, Int), String), message: String): (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long,Int), String) = {
    if (message == initialMsg){
      return (value._1,value._2,value._3,value._4,value._5,value._6)
    }else{
      var supEdgeset = new OpenHashSet[String]()
      var neighborSuperNodes = new String(value._5._1.toString+ "->")
      val msg= message.toString().split(":")
      // split message by ":"
      for (m <-msg) { // looping through the messages
        var ed = m.split("<-->")
        if (ed.size > 0 ){
          if( ed(0)==value._5._1.toString && !supEdgeset.contains(new String(ed(1)))) {
            supEdgeset.add(new String(ed(1)))
            neighborSuperNodes += ed(1) + "|"
          }
          else if (ed(1)==value._5._1.toString && !supEdgeset.contains(new String(ed(0)))){
            supEdgeset.add(new String(ed(0)))
            neighborSuperNodes+=ed(0)+"|"
          }
        }
        else{
        }
      }
      if(supEdgeset.size==0){
        return (value._1,value._2,value._3,value._4,value._5,new String(""))
      }
      else{
        return (value._1,value._2,value._3,value._4,value._5,neighborSuperNodes)
      }
    }
  }




  def sendMsgTrussForSuperNodes(triplet: EdgeTriplet[(Vert, Int , Map[Long,Map[Long,Int]] , Int, (Long,Int), String), Long]): Iterator[(VertexId, String)] = {
    val sourceVertex = triplet.srcAttr
    val destVertex=triplet.dstAttr
    var map:Map[Long,Map[Long,Int]] =sourceVertex._3
    //var map:Map[String,Int] <-
    var uns=getCoUn(sourceVertex._1.u.toString+","+sourceVertex._1.v.toString , destVertex._1.u.toString+","+destVertex._1.v.toString)._2 //uncommon vertex of source vertex
    var und=getCoUn(destVertex._1.u.toString+","+destVertex._1.v.toString , sourceVertex._1.u.toString+","+sourceVertex._1.v.toString)._2 //uncommon vertex of destination vertex
    var t = sourceVertex._3.get(uns).get(und)
    if(sourceVertex._2 == destVertex._2 && sourceVertex._2 <= t ) {
      if (sourceVertex._5._1 == -1) {                                                    //Supernode Id is initial value
        return Iterator((triplet.dstId, triplet.srcId +"T"+sourceVertex._2), (triplet.srcId, triplet.dstId+"T"+destVertex._2 ),(triplet.srcId, triplet.srcId +"T"+sourceVertex._2))
      }
      else
      {
        if (sourceVertex._5._1 != destVertex._5._1) {                                   //Supernode IDs are different
          return Iterator((triplet.dstId, sourceVertex._5._1 +"T"+sourceVertex._2), (triplet.srcId, destVertex._5._1+"T"+destVertex._2))
        } else {
          return Iterator.empty
        }
      }
    }
    else if (sourceVertex._2 == destVertex._2 && sourceVertex._2 > t ){                   // Src and Dst have same truss, but third edge has lower truss
      return Iterator((triplet.srcId,triplet.srcId+"T"+sourceVertex._2))
    }
    else if (sourceVertex._2 != destVertex._2  ){                                         // Src and Dst have different truss value
      if(sourceVertex._5._1 == -1){
        return Iterator((triplet.srcId,triplet.srcId+"T"+sourceVertex._2))
      } else {
        return Iterator.empty
      }
    }
    else{
      return Iterator((triplet.srcId,triplet.srcId+"T"+sourceVertex._2))
    }
  }


  def UpdateForSuperNodes(vertexId: VertexId, value: (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long,Int), String), message: String): (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long, Int), String) = {
    if (message == initialMsg){
      return (value._1,value._2,value._3,value._4,value._5,value._6)
    }else{
      var prevSNID  = value._5._1
      if(prevSNID == -1){ prevSNID = Long.MaxValue}
      var supNodeset = new mutable.HashSet[Long]()
      var trussnessOfSuperNode = value._5._2
      val msg= message.toString().split(":")                                                 // split message by ":"
      for (m <-msg) { // looping through the messages
        if(m!="") {
          val node = m.split("T")
          if (!supNodeset.contains(node(0).toLong)) {
            supNodeset.add(node(0).toLong)                                                  // adding supernode to supernodeset
          }
          trussnessOfSuperNode = min(node(1).toInt,trussnessOfSuperNode)
        }
      }
      if(supNodeset.size==0){
        return (value._1,value._2,value._3,value._4,value._5,value._6)
      }else if(supNodeset.size>=1){
        return (value._1,value._2,value._3,value._4,(min(supNodeset.min,prevSNID),trussnessOfSuperNode),value._6)
      }
      else{
        return (value._1,value._2,value._3,value._4,(supNodeset.min,trussnessOfSuperNode),value._6)
      }
    }
  }


  def getCoUn(first:String, second:String):(Long,Long) ={
    val a = first.split(",")
    val b = second.split(",")
    if(a.contains(b(0)))
    {
      return (b(0).toLong, b(1).toLong)
    }
    return (b(1).toLong , b(0).toLong)
  }

  def mergeMsgTruss(msg1: String, msg2:String): String = new String(msg1+":"+msg2)


  def countTrussness(M:Map[Long,Map[Long,Int]],k:Int):Int= {
    var count:Array[Int]=new Array[Int](k+1)
    for ((key,v) <- M)
    {
      if (v.size == 2){
        val tr=v.values.toList
        val j = min(k,min(tr(0),tr(1)))
        count(j)=count(j)+1
      }
    }
    for(i<-k to 3 by -1)
    {
      count(i-1)=count(i-1)+count(i)
    }
    var t=k
    while(t>2 && count(t)<t-2)
    {
      t = t-1
    }
    return t
  }

  def sendMsgTruss(triplet: EdgeTriplet[(Vert, Int , Map[Long,Map[Long,Int]] , Int,(Long, Int),String), Long]): Iterator[(VertexId, String)] = {
    val sourceVertex = triplet.srcAttr
    val destVertex=triplet.dstAttr

    if(sourceVertex._2!=sourceVertex._4 && destVertex._2!=destVertex._4)        //if truss is same as support for src and dst
    {
      return Iterator((triplet.dstId,sourceVertex._1.u+","+sourceVertex._1.v+"#"+sourceVertex._2),(triplet.srcId,destVertex._1.u+","+destVertex._1.v+"#"+destVertex._2))
    }else if(sourceVertex._2!=sourceVertex._4){
      return Iterator((triplet.dstId,sourceVertex._1.u+","+sourceVertex._1.v+"#"+sourceVertex._2))
    }else if(destVertex._2!=destVertex._4){
      return Iterator((triplet.srcId,destVertex._1.u+","+destVertex._1.v+"#"+destVertex._2))
    }else{
      return Iterator.empty
    }
  }



  def sendMsgTablet(triplet: EdgeTriplet[(Vert, Int , Map[Long,Map[Long,Int]] , Int,(Long, Int),String), Long]): Iterator[(VertexId, String)] = {
    return Iterator((triplet.dstId,triplet.srcAttr._1.u+","+triplet.srcAttr._1.v+"#"+triplet.srcAttr._2))
  }

  def updateTablet(vertexId: VertexId, value: (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long, Int),String),message: String): (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long, Int), String) = {

    if (message == initialMsg){
      return (value._1,value._2,value._3,value._4,value._5,value._6)
    }else {
      var tablet:Map[Long,Map[Long,Int]] = value._3
      val msg= message.toString().split(":")                                      //Split the messages received
      val cTruss=value._2                                                         //current truss value
      val cVert = value._1.u.toString()+","+value._1.v.toString()                 //current vertex
      for (m <-msg) {
        val (mVert, mTruss) = (m.split("#")(0), m.split("#")(1).toInt) // VErtex from message and it's truss value
        val (co, un) = getCoUn(cVert, mVert)
        if(tablet.contains(un))
        {
          val innerTab = tablet.get(un).head
          if(innerTab.contains(co))
          {
            if(innerTab.get(co).head > mTruss)                                  //Updating the minimal truss value for an edge
            {
              innerTab(co) = mTruss
              tablet(un) = innerTab
            }
          }else
          {
            innerTab += (co -> mTruss)
            tablet(un) = innerTab
          }
        }else{
          val innerTab = Map(co -> mTruss)
          tablet += (un -> innerTab)
        }
      }
      return (value._1, value._2, tablet, value._2, value._5, value._6)
    }
  }

  def vprogTruss(vertexId: VertexId, value: (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long, Int),String),message: String): (Vert,Int,Map[Long,Map[Long,Int]],Int,(Long, Int), String) = {

    if (message == initialMsg){
      return (value._1,value._2,value._3,value._4,value._5,value._6)
    }else{
      //var tablet:Map[String,Map[String,Int]]= value._3
      var tablet:Map[Long,Map[Long,Int]] =Map()                                   // Contains the information about the all neighboring edges and their trussness
      val cTruss=value._2                                                         //current truss value
      val cVert = value._1.u.toString()+","+value._1.v.toString()                 //current vertex
      val msg= message.toString().split(":")                                      //Split the messages received
      for (m <-msg){
        val (mVert , mTruss) = (m.split("#")(0) , m.split("#")(1).toInt)          // VErtex from message and it's truss value
        val (co , un) = getCoUn(cVert , mVert)                                    // Get the common and uncommon node from the current vertex and message vertex
        if(tablet.contains(un))
        {
          val innerTab = tablet.get(un).head
          if(innerTab.contains(co))
          {
            if(innerTab.get(co).head > mTruss)
            {
              innerTab(co) = mTruss
              tablet(un) = innerTab
            }
          }else
          {
            innerTab += (co -> mTruss)
            tablet(un) = innerTab
          }
        }else{
          val innerTab = Map(co -> mTruss)
          tablet += (un -> innerTab)
        }
      }
      val newTrussness=countTrussness(tablet,cTruss)
      if(newTrussness < cTruss)
      {
        return (value._1,newTrussness,tablet,cTruss,value._5,value._6)
      }
      return (value._1,value._2,tablet,value._2,value._5,value._6)
    }
  }




  def getTriangle[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED]):Graph[VertexSet, (Int,VertexSet)]={

    var nbrSets1 = graph.collectNeighborIds(EdgeDirection.Either)
    var nbrSets :VertexRDD[VertexSet] = nbrSets1.mapValues { (vid, nbrs) =>  //create sets of vid and their neighbor sets for each vertex
      val set = new VertexSet(nbrs.length)  //create a set of lenght of neighbors of the vertex
    var i = 0
      while (i < nbrs.length) {
        if (nbrs(i) != vid) {     //check if the neighbor vertex is the same as vertex id
          set.add(nbrs(i))        // adding the neighbor vertex to the set of neighbors of vertex
        }
        i += 1
      }
      set
    }



    var setGraph = graph.outerJoinVertices(nbrSets) {       //outer join of vertices of the graph and their neighbor sets
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    val finalGraph= setGraph.mapTriplets(triplet => {       //creating triplets
      val (smallSet, largeSet) = if (triplet.srcAttr.size < triplet.dstAttr.size) {
        (triplet.srcAttr, triplet.dstAttr)
      } else {
        (triplet.dstAttr, triplet.srcAttr)
      }
      val participatingSets = new VertexSet(smallSet.size)
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {      //iterating through the smaller neighbor set
        val vid = iter.next()
        if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {   //Checking if the current vid is in larger neighbor set and is not src or dst of the triplet
          participatingSets.add(vid)  //adding the vid to participatingSets
          counter=counter+1           //counter incremented
        }
      }
      (counter,participatingSets)        //number of common neighbors, set of common neighbors
    })

    finalGraph
  }

}



