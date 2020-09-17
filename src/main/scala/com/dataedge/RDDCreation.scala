package com.dataedge

import org.apache.spark.{SparkConf, SparkContext}

object RDDCreation {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("createRDD").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //first way to create RDD
    val data = (1 to 1000).toList
    val data_rdd = sc.parallelize(data)
    //data_rdd.foreach(println)

    //second way
    val rdd2 = sc.textFile("R:\\hadoop\\data\\json\\part-r-00000.json")
    rdd2.foreach(println)

    // third way: using existing rdd
    val rdd3 = data_rdd.map(e=>{e+100})
    rdd3.foreach(println)
  }

}
