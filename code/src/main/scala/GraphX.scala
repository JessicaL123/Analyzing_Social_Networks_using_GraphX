import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object GraphX {
  def main(args: Array[String]): Unit = {
        if (args.length != 2) {
          System.err.println("Usage: jarPath inputFile outputFile")
          System.exit(1)
        }

        val inputFile = args(0)
        val outputFile = args(1)
//    val inputFile = "file/soc-Epinions1.txt"
//    val outputFile = "file/output.txt"

    val sparkConf = new SparkConf().
      setAppName("grpahx")
//      .setMaster(sys.env.getOrElse("spark.master", "local[*]"))

    val sc = new SparkContext(sparkConf)

    // read the edges from file
    val input = sc.textFile(inputFile)
    val content = input.filter(x=> !x.contains("#")).map(_.split("""\t"""))

    // create vertices and edges
    val vertices = content.flatMap(x => x).distinct().map(v => (v.toLong, v))
    val edges = content.map(e => Edge(e(0).toLong, e(1).toLong, 1))
    val graph = Graph(vertices, edges)


    var res = ""
    res +=  "1. Find the top 5 nodes with the highest outdegree and ﬁnd the count of the number of outgoing edges in each\nNode:\toutDegrees:\n"
    graph.outDegrees.sortBy(_._2, ascending=false).take(5)
        .foreach(x => {
          res += x._1 + "\t" + x._2 + "\n"
        })

    res +=  "\n2. Find the top 5 nodes with the highest indegree and ﬁnd the count of the number of incoming edges in each\nNode:\tinDegrees:\n"
    graph.inDegrees.sortBy(_._2, ascending=false).take(5)
        .foreach(x => {
          res += x._1 + "\t" + x._2 + "\n"
        })

    res += "\n3. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values. You are free to deﬁne the threshold parameter.\nNode:\tPageRank:\n"
    graph.pageRank(0.1).vertices.sortBy(_._2, ascending=false).take(5)
        .foreach(x => {
          res += x._1 + "\t" + x._2 + "\n"
        })

    res += "\n4. Run the connected components algorithm on it and ﬁnd the top 5 components with the largest number of nodes.\nNode:\tconnectedComponent:\n"
    graph.connectedComponents().vertices.sortBy(_._2, ascending=false).take(5)
      .foreach(x => {
        res += x._1 + "\t" + x._2 + "\n"
      })

    res += "\n5. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count. In case of ties, you can randomly select the top 5 vertices.\nNode:\ttrangleCounts:\n"
    graph.triangleCount().vertices.sortBy(_._2, ascending=false).take(5)
      .foreach(x => {
        res += x._1 + "\t" + x._2 + "\n"
      })

    sc.parallelize(Seq(res)).saveAsTextFile(outputFile)
  }
}
