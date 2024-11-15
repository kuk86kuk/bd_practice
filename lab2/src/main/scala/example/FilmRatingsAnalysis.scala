import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import org.apache.spark.rdd.RDD


object FilmRatingsAnalysis {
  def main(args: Array[String]): Unit = {
    // Инициализация SparkConf и SparkContext
    val conf = new SparkConf().setAppName("First5Records").setMaster("local")
    val sc = new SparkContext(conf)

    // 
    val filePath = "data/u.data"
    val initialCount: Map[Int, Int] = Map()
    val addToCounts = (counts: Map[Int, Int], value: Int) => {
      counts + (value -> (counts.getOrElse(value, 0) + 1))}
    val mergeMaps = (p1: Map[Int, Int], p2: Map[Int, Int]) => {
      p1 ++ p2.map { case (k, v) => k -> (v + p1.getOrElse(k, 0)) }}

    // Основная логика
    val data: RDD[String] = sc.textFile(filePath)
    val pairRDD: RDD[(String, Int)] = data.map(line => {
          val parts = line.split("\t")
          (parts(1), parts(2).toInt)
        })    
    val countedRDD: RDD[(String, Map[Int, Int])] = pairRDD.aggregateByKey(initialCount)(addToCounts, mergeMaps)
    countedRDD.collect().foreach(printStat)
    val allMarks: Array[(String, Map[Int, Int])] = pairRDD.map { case (_, rating) => ("ALL", rating) }
      .aggregateByKey(initialCount)(addToCounts, mergeMaps)
      .collect()
    allMarks.foreach(printStat)

    // Остоновка spark сессии
    sc.stop()
  }

  def printStat(inp: (String, Map[Int, Int])): Unit = {
      val (ind, marks) = inp
      val marksList = (1 to 5).map(i => marks.getOrElse(i, 0))
      println(s"Marks for film $ind: 1 -> ${marksList(0)}, 2 -> ${marksList(1)}, 3 -> ${marksList(2)}, 4 -> ${marksList(3)}, 5 -> ${marksList(4)}")
    }
}