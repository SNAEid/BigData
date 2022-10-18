
import org.apache.spark
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // creating spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark example")
      .getOrCreate()

    // create spark context
    val sc = spark.sparkContext


    ////////////////////////////////////////////////////////////////////////////////////////////////
    //to read the content of the files into a single RDD (using "" + )
    val filesContentRdd = sc.wholeTextFiles("src/documents/text01.txt," + "src/documents/text02.txt," + "src/documents/text03.txt")
    //filesContentRdd.foreach(println)

    // for multi files we can use *  from a directory into a single RDD
    //val filesContentRDD = sc.textFile("src/m1/spark/documents/*")



    def InvertedIndex(): Unit ={
      val splitRdd = filesContentRdd.flatMapValues(line => line.split(" "))
      val mapingOperations = splitRdd.map(x => (x._2, (1, List(x._1.split("/").last))))
      val crateTuples = mapingOperations.reduceByKey((x, y) => ((x._1 + x._1), List.concat(x._2, y._2)))


      val sortAlphabetically = crateTuples.map(sort => (sort._1, sort._2)).sortByKey()
      println("**********| Inverted Index for the files |**********\n")
      val fullOperation = sortAlphabetically.foreach(println)
      return fullOperation
      //sortAlphabetically.saveAsTextFile("src/documents/wholeInvertedIndex.txt")

    }
    val invertedIndex  = InvertedIndex()


    ////////////////////////////////////////////////////////////////////////////////////////
    //*************************| Query Processing |*******************************

    //read wholeInvertedIndex txt file in rdd
    //
    val rdd2 = sc.textFile("src/documents/wholeInvertedIndex.txt")

    val searchRDD = rdd2.flatMap(line => (line.split(",")(1)))

    //rdd2.filter(line => rdd2.contains("string to search"))

    println("Type word  : ")
    val input = scala.io.StdIn.readLine()
    //val input: String = "omar sana"

    val mylist: List[String] = input.split(" ").toList
    //println("My list ", mylist)


    for (element <- mylist) {
      val result = rdd2.map(x => x.toLowerCase)
        .filter(x => x.contains(element)).map(s => s.split(",")(2))

      result.take(100).foreach(println)
    }

    }

}