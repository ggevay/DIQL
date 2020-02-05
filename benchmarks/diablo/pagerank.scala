import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util._
import scala.annotation.tailrec


object PageRank {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val vertices = args(1).toLong
    val edges = args(2).toLong
    val num_steps = 1

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()
    val RMATa = 0.30
    val RMATb = 0.25
    val RMATd = 0.25
    val RMATc = 0.20

    val vn = math.round(Math.pow(2.0,Math.ceil(Math.log(vertices)/Math.log(2.0)))).toInt

    def pickQuadrant ( a: Double, b: Double, c: Double, d: Double ): Int
      = rand.nextDouble() match {
          case x if x < a => 0
          case x if (x >= a && x < a + b) => 1
          case x if (x >= a + b && x < a + b + c) => 2
          case _ => 3
        }

    def chooseCell ( x: Int, y: Int, t: Int ): (Int,Int) = {
        if (t <= 1)
           (x,y)
        else {
           val newT = math.round(t.toFloat/2.0).toInt
           pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
             case 0 => chooseCell(x, y, newT)
             case 1 => chooseCell(x + newT, y, newT)
             case 2 => chooseCell(x, y + newT, newT)
             case 3 => chooseCell(x + newT, y + newT, newT)
           }
        }
    }

    def addEdge ( vn: Int ): (Int,Int) = {
       val v = math.round(vn.toFloat/2.0).toInt
       chooseCell(v,v,v)
    }

    val E = sc.parallelize(1L to edges/100)
              .flatMap{ i => (1 to 100).map{ j => addEdge(vn) } }
              .map{ case (i,j) => ((i.toLong,j.toLong),true) }
              .cache()

    val size = sizeof(((1L,1L),true))
    println("*** %d %d  %.2f GB".format(vertices,edges,edges.toDouble*size/(1024.0*1024.0*1024.0)))

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
      val links = E.map(_._1).groupByKey().cache()
      var ranks = links.mapValues(v => 1.0/vertices)

      for (i <- 1 to num_steps) {
          val contribs = links.join(ranks).values.flatMap {
                              case (urls, rank)
                                => val size = urls.size
                                   urls.map(url => (url, rank / size))
                         }
          ranks = contribs.reduceByKey(_ + _).mapValues(0.15/vertices + 0.85 * _)
      }
      println(ranks.count)

      println("**** PagerankSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var P: vector[Double] = vector();
         var C: vector[Int] = vector();
         var N: Long = vertices;
         var b: Double = 0.85;

         for i = 1, N do {
             C[i] := 0;
             P[i] := 1.0/N;
         };

         for i = 1, N do
             for j = 1, N do
                if (E[i,j])
                   C[i] += 1;

         var k: Int = 0;

         while (k < num_steps) {
           var Q: matrix[Double] = matrix();
           k += 1;
           for i = 1, N do
             for j = 1, N do
                 if (E[i,j])
                    Q[i,j] := P[i];
           for i = 1, N do
               P[i] := (1-b)/N;
           for i = 1, N do
               for j = 1, N do
                   P[i] += b*Q[j,i]/C[j];
         };

         println(P.count());

        """)

      println("**** PagerankDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
   }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
