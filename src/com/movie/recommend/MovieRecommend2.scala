package com.movie.recommend

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

object MovieRecommend2 {

  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (movieId, userId, ratings)

  }
  
  def parseMovieFile(line : String) : (Long , String) = {
    
    val moviesRecord = line.split(",").toList
    
    val movieId = moviesRecord(0).toLong
    
    def removeFirstAndLast[A](xs: Iterable[A]) = xs.drop(1).dropRight(1)
    
    def parseMovieName(list: List[String]): String = {
      removeFirstAndLast(list).mkString(",").replaceAll("^\"", "").replaceAll("\"$", "")
    }
    
    val movieName = if(moviesRecord.length > 3) {
      parseMovieName(moviesRecord)
    }else{
      moviesRecord(1)
    }

    
    (movieId , movieName)
    
  }
  
  def matrixMultiplication(m1: RDD[(Long, Long, Double)], m2: RDD[(Long, Long, Double)]): RDD[((Long, Long), Double)] = {

    val joinedMatrix = m1.map(mElement => (mElement._2, mElement))
      .join(m2.map(nElement => (nElement._1, nElement))).
      flatMap {
        case (k, (mElement, nElement)) => if (mElement._1 >= nElement._2)
          Some(((mElement._1, nElement._2), mElement._3 * nElement._3))
        else
          None
      }
    val aggregateMatrix = joinedMatrix.reduceByKey((x, y) => x + y)

    aggregateMatrix

  }

  def generatePredictionMatrix(userMatrix: RDD[((Long, Long), Double)], itemToItemMatrix: RDD[((Long, Long), Double)]): RDD[((Long, Long), Double)] = {

    val joinedMatrix = userMatrix.map(mElement => (mElement._1._2, mElement))
      .join(itemToItemMatrix.map(nElement => (nElement._1._1, nElement))).
      map { case (k, (mElement, nElement)) => ((mElement._1._1, nElement._1._2), (mElement._2 * nElement._2, nElement._2)) }
    val aggregateMatrix = joinedMatrix.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(tuple => tuple._1 / tuple._2)

    aggregateMatrix

  }

  def main(args: Array[String]) {

    val t1 = System.currentTimeMillis() / 1000
    val userId = args(0).toLong
    val similarityThreshold = args(1).toDouble

    val conf = new SparkConf().setAppName("Movie Recommendation")
    conf.setMaster("local[2]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "8g")
      .set("spark.executor.heartbeatInterval", "60")
      .set("spark.network.timeout", "600")
      
    val sparkContextObject = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContextObject)

  
    
    val matrix1 = sparkContextObject
    .textFile("ratings.csv")
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map(parseMatrix)
    .cache()

    
    val movieNamesList = sparkContextObject.textFile("movies.csv")
                                      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
                                      .map(parseMovieFile)

    
    val matrix2 = matrix1.map(tuple => {
      (tuple._2, tuple._1, tuple._3)
    })

    val m1m2DotProduct: RDD[((Long, Long), Double)] = matrixMultiplication(matrix1, matrix2).
      cache()

    val diagonal: Map[Long, Double] = m1m2DotProduct.
      filter(tuple => tuple._1._1 == tuple._1._2).
      map(tuple => (tuple._1._1, scala.math.sqrt(tuple._2)))
      .collect()
      .toMap

    val bDiagonal = sqlContext.sparkContext.broadcast(diagonal)

    val itemToItemMatrix: RDD[((Long, Long), Double)] = m1m2DotProduct.map(t => {

      val ((m1, m2), score) = t
      val m1Sqrt = bDiagonal.value.apply(m1)
      val m2Sqrt = bDiagonal.value.apply(m2)
      val cosinSimilarity = score / (m1Sqrt * m2Sqrt)
      ((m1, m2), cosinSimilarity)
    })
    //.cache()

    val itemToItemMatrixTemp = itemToItemMatrix
      .filter(tuple => {
        (tuple._2 >= similarityThreshold)
      })
      .flatMap(j => {
        val ((m1, m2), cosinSimilarity) = j

        if (m1 != m2)
          Seq(
            ((m1, m2), cosinSimilarity),
            ((m2, m1), cosinSimilarity))
        else
          Seq.empty[((Long , Long) , Double)]
      })

    val userFilteredMatrix = matrix2.filter(tuple => {
      tuple._1 == userId
    })
      .map(t => ((t._1, t._2), t._3))

    val predictionMatrix: RDD[((Long, Long), Double)] = generatePredictionMatrix(userFilteredMatrix, itemToItemMatrixTemp)

    val predictions = predictionMatrix
      .leftOuterJoin(userFilteredMatrix)
      .filter(t => {
        val ((u, m), (pr, ur)) = t
        ur match {
          case Some(r) => false
          case None    => true
        }
      })
      .map(t => {
        val ((u, m), (pr, ur)) = t
        (u, (m, pr))
      })
      .groupByKey
      .map(t => {
        val (u, itr) = t
        
        val list = itr.toList.sortWith((a, b) => a._2 > b._2).take(10)
        (u, list)
      })
      .flatMap(tuple => {
        val movieList = tuple._2
        val userId = tuple._1
        movieList.map(t => (t._1, (userId, t._2)))
      })
      
    val finalMovieList = movieNamesList.map(movieTuple => {
      (movieTuple._1 , movieTuple._2)
    }).join(predictions.map(ratingTuple => {
      (ratingTuple._1 , ratingTuple._2)
    })).map {
      case (mid , (movieName , (userId, recScore))) => {
        (userId, (mid, recScore, movieName))
      }
    }
    .groupByKey
     .map(t => {
        val (u, itr) = t
        val list = itr.toList.sortWith((a, b) => a._2 > b._2).map(_.productIterator.mkString(":")).mkString("\n")
        s"${u}\n${list}"
      })

    finalMovieList.saveAsTextFile("recommendation")
    
   val ratedMovieList = movieNamesList.map(movieTuple => {
      (movieTuple._1 , movieTuple._2)
    }).join(userFilteredMatrix.map(ratingTuple => {
      (ratingTuple._1._2 , (ratingTuple._1._1, ratingTuple._2))
    })).map {
      case (mid , (movieName , (userId, recScore))) => {
        (userId, (mid, recScore, movieName))
      }
    }
    .groupByKey
     .map(t => {
        val (u, itr) = t
        val list = itr.toList.sortWith((a, b) => a._3 > b._3).map(_.productIterator.mkString(":")).mkString("\n")
        s"${u}\n${list}"
      })

    ratedMovieList.saveAsTextFile("ratedmovies")

    val t2 = System.currentTimeMillis() / 1000

    println(s"time take:${t2 - t1}")

  }

}