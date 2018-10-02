
import org.apache.spark._
import org.apache.log4j._

/** Find the movies with the most ratings. */
object PopularMovies {

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")   
    
    // Read in each rating line
    val lines = sc.textFile("../../ml-100k/u.data")
    
    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (
      x.split("\t")(1).toInt,
      1
    ))
    
    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )

    // Sort
    val sortedMovies = movieCounts
      .map( x => (x._2, x._1) )
      .sortByKey()
    
    // Collect and print results
    val results = sortedMovies.collect()
    results.foreach(println)
  }
  
}

