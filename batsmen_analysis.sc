spark-shell --conf spark.sql.catalogImplementation=in-memory
import spark.implicits._

val ipl = spark.read.option("header",true).csv("C:/Users/Dharsankumar/Desktop/Semester 5/IPL dataset/2021/IPL Ball-by-Ball Dataset 2021.csv")
ipl.show
/*
+-------+------+----+----+---------+-----------+--------------+------------+----------+----------+------------+---------+--------------+----------------+-------+-----------+--------------+--------------------+
|     id|inning|over|ball|  batsman|non_striker|        bowler|batsman_runs|extra_runs|total_runs|non_boundary|is_wicket|dismissal_kind|player_dismissed|fielder|extras_type|  batting_team|        bowling_team|
+-------+------+----+----+---------+-----------+--------------+------------+----------+----------+------------+---------+--------------+----------------+-------+-----------+--------------+--------------------+
|1254058|     1|   0|   1|RG Sharma|    CA Lynn|Mohammed Siraj|           2|         0|         2|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   0|   2|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   0|   3|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   0|   4|RG Sharma|    CA Lynn|Mohammed Siraj|           2|         0|         2|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   0|   5|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   0|   6|RG Sharma|    CA Lynn|Mohammed Siraj|           1|         0|         1|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   1|   1|RG Sharma|    CA Lynn|   KA Jamieson|           1|         0|         1|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   1|   2|  CA Lynn|  RG Sharma|   KA Jamieson|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   1|   3|  CA Lynn|  RG Sharma|   KA Jamieson|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   1|   4|  CA Lynn|  RG Sharma|   KA Jamieson|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   1|   5|  CA Lynn|  RG Sharma|   KA Jamieson|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   1|   6|  CA Lynn|  RG Sharma|   KA Jamieson|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   2|   1|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   2|   2|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   2|   3|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   2|   4|RG Sharma|    CA Lynn|Mohammed Siraj|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   2|   5|RG Sharma|    CA Lynn|Mohammed Siraj|           4|         0|         4|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   2|   6|RG Sharma|    CA Lynn|Mohammed Siraj|           2|         0|         2|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   3|   1|  CA Lynn|  RG Sharma|     YS Chahal|           0|         0|         0|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
|1254058|     1|   3|   2|  CA Lynn|  RG Sharma|     YS Chahal|           4|         0|         4|           0|        0|            NA|              NA|     NA|         NA|Mumbai Indians|Royal Challengers...|
+-------+------+----+----+---------+-----------+--------------+------------+----------+----------+------------+---------+--------------+----------------+-------+-----------+--------------+--------------------+
only showing top 20 rows*/

//Batsmen - Most Runs:
//--------------------
import org.apache.spark.sql.functions.col
val all_runs = ipl.select(col("batsman"),col("total_runs").alias("runs"))
val all_runs2 = all_runs.rdd.map { x => (x(0).toString, x(1).toString.t) }
val all_runs3 = all_runs2.reduceByKey(_+_).sortBy(_._1)
val all_balls = ipl.groupBy("batsman").count().toDF
val all_balls2 = all_balls.rdd.map { x => (x(0).toString, x(1).toString.toInt) }.sortBy(_._1)

val most_runs = all_runs3.join(all_balls2).map{ case(x,(y1,y2)) => (x,y1,y2) }.sortBy(_._2, false)
val most_runs2 = most_runs.map{ case(x,y1,y2) => (x,y1,y2, (y1.toFloat*100/y2.toFloat).toInt)}
most_runs2.toDF("batsman","runs","balls","strike rate").show
/*
+--------------+----+-----+-----------+
|       batsman|runs|balls|strike rate|
+--------------+----+-----+-----------+
|      S Dhawan| 270|  185|        145|
|      KL Rahul| 247|  188|        131|
|  F du Plessis| 224|  158|        141|
|   JM Bairstow| 216|  152|        142|
|     RG Sharma| 210|  155|        135|
|    GJ Maxwell| 208|  141|        147|
|     SV Samson| 201|  137|        146|
|        N Rana| 197|  156|        126|
|    D Padikkal| 185|  115|        160|
|   RA Tripathi| 174|  121|        143|
|    MA Agarwal| 168|  129|        130|
|       PP Shaw| 167|  105|        159|
|      SA Yadav| 158|  110|        143|
|       V Kohli| 156|  120|        130|
|        MM Ali| 145|   91|        159|
|     DA Warner| 143|  125|        114|
|    AD Russell| 132|   85|        155|
|AB de Villiers| 131|   77|        170|
|       RR Pant| 131|   99|        132|
|    RD Gaikwad| 123|  106|        116|
+--------------+----+-----+-----------+
only showing top 20 rows*/


val most_runs_batsman = (most_runs2.take(1))(0)
//most_runs_batsman: (String, Int, Int, Int) = (S Dhawan,270,185,145)
val most_balls_batsman = (most_runs2.take(1))(0)
//most_balls_batsman: (String, Int, Int, Int) = (S Dhawan,270,185,145)
val most_sr_batsman = (most_runs2.filter(x=> x._3>10).sortBy(_._4, false).take(1))(0)
//most_sr_batsman: (String, Int, Int, Int) = (SM Curran,53,26,203)
