import Test.{Customer, Order}
import org.apache.spark.SparkContext
import anonymous.Util._
import edu.uta.diql._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  case class Customer ( name: String, cid: Int, account: Float )

  case class Order ( oid: Int, cid: Int, price: Float )

  def main(args: Array[String]): Unit = {

    val skewed = args.length == 3

    val totalSize = args(0).toLong
    val numGroups = args(1).toInt
    val exponent =
      if (skewed) {
        args(2).toDouble
      } else {
        0
      }

    implicit val sc: SparkContext = sparkSetup(this)

    val visitsAll =
      if (skewed) {
        getBounceRateSkewedRandomInput(totalSize, numGroups, exponent)
      } else {
        getBounceRateRandomInput(totalSize, numGroups)
      }

    val stopWatch = new StopWatch

    explain(true)

    // NumBounces
//    q("""
//      select (
//        (count/
//          (select vid
//          from (vid,c) <- (select (v,1L) from v <- visits)
//          group by vid
//          having count/c == 1)
//        )
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)

    //Scala code (outer parallel):
//    visitsAll.groupByKey().map({
//      case scala.Tuple2((gid @ _), (diql$1 @ _)) => diql$1.flatMap(((x$macro$74: Long) => x$macro$74 match {
//        case (diql$7 @ _) => {
//          val x$4 = (((scala.Tuple2(diql$7, 1L): @scala.unchecked): scala.Tuple2[Long, Long]) match {
//            case scala.Tuple2((vid @ _), (c @ _)) => scala.Tuple2(vid, c)
//          });
//          val vid = x$4._1;
//          val c = x$4._2;
//          List(scala.Tuple2(vid, c))
//        }
//      })).groupBy(((x$7) => x$7._1)).mapValues(((x$6) => x$6.map(((x$5) => x$5._2)))).flatMap(((x$macro$78: scala.Tuple2[Long, Traversable[Long]]) => x$macro$78 match {
//        case scala.Tuple2((diql$3 @ _), (diql$4 @ _)) => if (diql$4.map(((x$macro$81: Long) => x$macro$81 match {
//          case (diql$5 @ _) => 1L
//        })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).==(1))
//          List(1L)
//        else
//          Nil
//      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y)))
//    })

//    q("""
//      select (
//        (count/
//          (select vid
//          from (vid,c) <- (select (v,1L) from v <- visits)
//          group by vid)
//        )
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)
//
//    // Scala code:
//    visitsAll.groupByKey().map({
//      case scala.Tuple2((gid @ _), (diql$1 @ _)) => diql$1.flatMap(((x$macro$50: Long) => x$macro$50 match {
//        case (diql$6 @ _) => {
//          val x$4 = (((scala.Tuple2(diql$6, 1L): @scala.unchecked): scala.Tuple2[Long, Long]) match {
//            case scala.Tuple2((vid @ _), (c @ _)) => scala.Tuple2(vid, c)
//          });
//          val vid = x$4._1;
//          val c = x$4._2;
//          List(scala.Tuple2(vid, ()))
//        }
//      })).groupBy(((x$7) => x$7._1)).mapValues(((x$6) => x$6.map(((x$5) => x$5._2)))).map(((x$macro$54: scala.Tuple2[Long, Traversable[Unit]]) => x$macro$54 match {
//        case scala.Tuple2((diql$3 @ _), (diql$4 @ _)) => 1L
//      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y)))
//    })



//    q("""
//      select (
//        (count/visits)
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)
//
//    //Scala code (flat):
//    visitsAll.mapValues({
//      case (diql$2 @ _) => 1L
//    }).reduceByKey({
//      case scala.Tuple2((x @ _), (y @ _)) => ((x: Long, y: Long) => x.+(y))(x, y)
//    }).map({
//      case scala.Tuple2((gid @ _), (diql$3 @ _)) => diql$3
//    })

    q("""
      select (
        (count/
          (select distinct v from v <- visits)
        )
      )
      from (gid,visits) <- visitsAll
      group by gid
      """).foreach(println)

    visitsAll.groupByKey().map({
      case scala.Tuple2((gid @ _), (diql$1 @ _)) => diql$1.map(((x$macro$44: Long) => x$macro$44 match {
        case (diql$6 @ _) => scala.Tuple2(diql$6, 0)
      })).groupBy(((x$3) => x$3._1)).mapValues(((x$2) => x$2.map(((x$1) => x$1._2)))).map(((x$macro$46: scala.Tuple2[Long, Traversable[Int]]) => x$macro$46 match {
        case scala.Tuple2((diql$4 @ _), _) => 1L
      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y)))
    })

    stopWatch.done()
  }
}
