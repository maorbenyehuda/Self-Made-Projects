package com.imdb.analysis;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import scala.Tuple2;
import java.io.Serializable;
import java.util.Comparator;
public class IMDBAnalysis {
	// Serializable Comparator for Integer
	public static class SerializableComparator implements Comparator<Integer>, Serializable {
	    @Override
	    public int compare(Integer o1, Integer o2) {
	        return o1.compareTo(o2);
	    }
	}

	// Static method to compare ratings
	public static int compareRatings(Tuple2<String, Double> a, Tuple2<String, Double> b) {
	    return Double.compare(a._2, b._2);
	}
	

	

	
    public static void main(String[] args) {

    	// Set Up Spark Configuration and Context
        SparkConf conf = new SparkConf().setAppName("IMDBAnalysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load Data from CSV
        String filePath = "excelfile/IMBD.csv";
        JavaRDD<String> lines = sc.textFile(filePath);

        // Skip Header
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Split Rows by Comma, Considering Quotes
        JavaRDD<String[]> rows = data.map(line -> line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"));

        // Filter Out Rows with Missing Essential Columns
        JavaRDD<String[]> cleanedRows = rows.filter(fields -> 
            fields.length == 9 && // Ensure all columns are present
            !fields[0].isEmpty() &&  // title
            !fields[1].isEmpty() &&  // year
            !fields[8].isEmpty()     // votes
        );

        // Convert Votes Column to Numeric (Remove Commas)
        JavaRDD<String[]> votesCleaned = cleanedRows.map(fields -> {
        	fields[8] = fields[8].replaceAll("[^0-9]", ""); // Keep only digits
            return fields;
        });

        JavaRDD<String[]> starsCleaned = votesCleaned.map(fields -> {
            if (fields.length > 7 && fields[7] != null) {
                // Remove unwanted characters and split properly
                String cleanedStars = fields[7]
                    .replaceAll("\\|", "")          // Remove pipe symbols
                    .replaceAll("Stars:", "")       // Remove "Stars:" label
                    .replaceAll("'", "")            // Remove single quotes
                    .trim();                        // Trim spaces
                
                // Ensure consistent separation using only commas
                String[] parts = cleanedStars.split(",\\s*");
                
                // Join cleaned names back together
                fields[7] = String.join(", ", parts);
            }
            return fields;
        });



        
        // Extract Numeric Year (Take First Year for Ranges)
        JavaRDD<String[]> yearCleaned = starsCleaned.map(fields -> {
            String year = fields[1];

            // Debug: Show raw bytes of the year field to check hidden characters
            System.out.print("Raw Bytes: ");
            for (byte b : year.getBytes()) {
                System.out.print(b + " ");
            }
            System.out.println();

            // Normalize to remove any hidden Unicode characters
            year = java.text.Normalizer.normalize(year, java.text.Normalizer.Form.NFKC);

            // Remove all types of brackets
            year = year.replaceAll("[\\(\\)\\[\\]\\{\\}<>]", "").trim();

            // Handle TV Shows and Movies Separately
            if (year.contains("–")) {
                // If it's a TV Show with a range (e.g., 2015–2022 or 2018–), keep the dash
                year = year.trim();
            } else {
                // Else, it's a Movie - only keep the numeric part
                year = year.replaceAll("[^0-9]", "");
            }

            // If year is still empty, set as "Unknown"
            if (year.isEmpty()) {
                year = "Unknown";
            }

            // Keep the cleaned year
            fields[1] = year;
            return fields;
        });

        // Display Cleaned Take 10 Results
        yearCleaned.take(10).forEach(fields -> System.out.println("Title: " + fields[0] + ", Year: " + fields[1] + ", Votes: " + fields[8]));

        //movies and tv shows rdds
        JavaRDD<String[]> tvShowsRDD = yearCleaned.filter(fields -> fields[1].contains("–"));
        JavaRDD<String[]> moviesRDD = yearCleaned.filter(fields -> !fields[1].contains("–"));
     
      
     // Top Rated Movies by Genre
        System.out.println("\nTask 2: Top Rated Movies by Genre");

     //  Split Genre Column into Individual Genres
        JavaPairRDD<String, Tuple2<String, Double>> genreWithTitleAndRating = moviesRDD.flatMapToPair(fields -> {
            List<Tuple2<String, Tuple2<String, Double>>> genrePairs = new ArrayList<>();
            String title = fields[0];
            double rating = fields[5].isEmpty() ? 0.0 : Double.parseDouble(fields[5]);
            String[] genres = fields[4].split(",\\s*"); // Split by comma and optional space
            for (String genre : genres) {
                genrePairs.add(new Tuple2<>(genre, new Tuple2<>(title, rating)));
            }
            return genrePairs.iterator();
        });

        // Group by Genre and Sort by Rating in Descending Order
        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> groupedByGenre = genreWithTitleAndRating
            .groupByKey()
            .mapValues(movies -> {
                List<Tuple2<String, Double>> movieList = new ArrayList<>();
                movies.forEach(movieList::add);
                movieList.sort((a, b) -> Double.compare(b._2(), a._2())); // Sort by rating descending
                return movieList;
            });

     // Extract Top 10 Movies for Each Genre
        JavaPairRDD<String, List<Tuple2<String, Double>>> top10ByGenre = groupedByGenre.mapValues(movies -> {
            List<Tuple2<String, Double>> movieList = new ArrayList<>();
            movies.forEach(movieList::add);
            movieList.sort((a, b) -> Double.compare(b._2(), a._2())); // Sort by rating descending
            // Create a new ArrayList to avoid serialization issue
            return new ArrayList<>(movieList.subList(0, Math.min(10, movieList.size())));
        });

        // Display the results
        top10ByGenre.collect().forEach(entry -> {
            System.out.println("Genre: " + entry._1());
            entry._2().forEach(movie -> 
                System.out.println("    Title: " + movie._1() + ", Rating: " + movie._2())
            );
        });




     
     // Actor Collaboration Network
        System.out.println("\nTask 3: Actor Collaboration Network");

        // Step 1: Extract Only (Title, Year, Stars) into a New RDD
        JavaRDD<Tuple3<String, String, String>> movieInfoRDD = moviesRDD
            .map(fields -> {
                if (fields.length < 8 || fields[7].trim().isEmpty() || fields[0].trim().isEmpty() || fields[1].trim().isEmpty()) {
                    return null; // Ignore invalid rows
                }
                // Trim all fields
                String title = fields[0].trim();
                String year = fields[1].trim();
                String stars = fields[7].replaceAll("\\|", "").replaceAll("Stars:", "").trim();

                // Normalize spaces and remove extra commas
                stars = Arrays.stream(stars.split("\\s*,\\s*"))
                              .map(String::trim)
                              .filter(s -> !s.isEmpty())
                              .distinct()
                              .collect(Collectors.joining(", ")); // Join back into a clean string

                return new Tuple3<>(title, year, stars);
            })
            .filter(Objects::nonNull);

        // Apply distinct() on the new RDD
        JavaRDD<Tuple3<String, String, String>> uniqueMoviesRDD = movieInfoRDD.distinct();

        // Extract Actor Names and Movie Title
        JavaRDD<Tuple2<String, List<String>>> movieActors = uniqueMoviesRDD
            .map(tuple -> {
                String movieTitle = tuple._1();
                List<String> actors = Arrays.asList(tuple._3().split(", ")); // Split by clean comma
                return new Tuple2<>(movieTitle, actors);
            })
            .filter(tuple -> tuple._2.size() > 1); // Only consider movies with multiple actors

        // Generate Pairwise Combinations with Movie Name
        JavaRDD<Tuple2<Tuple2<String, String>, String>> coOccurrences = movieActors.flatMap(movie -> {
            String movieTitle = movie._1;
            List<String> actors = movie._2;
            List<Tuple2<Tuple2<String, String>, String>> pairs = new ArrayList<>();
            for (int i = 0; i < actors.size(); i++) {
                for (int j = i + 1; j < actors.size(); j++) {
                    // Ensure consistent ordering of pairs
                    String actor1 = actors.get(i);
                    String actor2 = actors.get(j);
                    if (actor1.compareTo(actor2) < 0) {
                        pairs.add(new Tuple2<>(new Tuple2<>(actor1, actor2), movieTitle));
                    } else {
                        pairs.add(new Tuple2<>(new Tuple2<>(actor2, actor1), movieTitle));
                    }
                }
            }
            return pairs.iterator();
        });

        // Count Collaborations and Collect Movie Titles
        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, List<String>>> collaborations = coOccurrences
            .mapToPair(pair -> {
                Tuple2<String, String> actors = pair._1;
                String movieTitle = pair._2;
                List<String> moviesList = new ArrayList<>();
                moviesList.add(movieTitle);
                return new Tuple2<>(actors, new Tuple2<>(1, moviesList));
            })
            .reduceByKey((tuple1, tuple2) -> {
                // Sum the counts and merge movie title lists
                int count = tuple1._1 + tuple2._1;
                List<String> mergedMovies = new ArrayList<>(tuple1._2);
                mergedMovies.addAll(tuple2._2);
                return new Tuple2<>(count, mergedMovies);
            });

        // Collect and Print the Results
        List<Tuple2<Tuple2<String, String>, Tuple2<Integer, List<String>>>> collabList = collaborations.collect();
        collabList.forEach(pair -> {
            Tuple2<String, String> actors = pair._1;
            Integer count = pair._2._1;
            List<String> movies = pair._2._2;
            System.out.println(actors._1 + " & " + actors._2 + " -> " + count + " collaborations");
            System.out.println("Movies: " + String.join(", ", movies));
            System.out.println("-------------------------------------------------");
        });







        
     // Task 4: High-Rated Hidden Gems
        System.out.println("\nTask 4: High-Rated Hidden Gems");

     // Filter movies with high ratings and low votes
        JavaRDD<String[]> hiddenGems = moviesRDD
            .filter(row -> {
                String ratingStr = row[5].replaceAll("\"", "").trim();
                String votesStr = row[8].replaceAll("\"", "").trim();
                
                // Check if both rating and votes are numeric
                if (!ratingStr.matches("\\d+(\\.\\d+)?") || !votesStr.matches("\\d+")) {
                    return false; // Skip rows with invalid data
                }
                
                double rating = Double.parseDouble(ratingStr);
                int votes = Integer.parseInt(votesStr);
                return rating > 8.0 && votes < 10000;
            })
            .sortBy(row -> Double.parseDouble(row[5].replaceAll("\"", "").trim()), false, 1); // Sort by rating descending


        // Display the top 20 hidden gems
        System.out.println("Top 20 High-Rated Hidden Gems:");
        hiddenGems
            .take(20)
            .forEach(row -> 
                System.out.println("Title: " + row[0] + ", Year: " + row[1] + 
                                   ", Rating: " + row[5] + ", Votes: " + row[8])
            );


     // Task 5: Word Frequency in Movie Titles
        System.out.println("\nTask 5: Word Frequency in Movie Titles");

        // List of common stop words to exclude
        List<String> stopWords = Arrays.asList(
            "the", "and", "of", "in", "a", "to", "is", "it", "for", 
            "on", "with", "as", "by", "an", "from", "this", "that", 
            "at", "be", "are", "was", "were", "or", "but", "not"
        );

        // Perform word count on titles
        JavaPairRDD<String, Integer> wordCount = moviesRDD
            .flatMapToPair(row -> {
                String title = row[0].toLowerCase();
                
                // Split title into words and filter out stop words
                String[] words = title.split("\\W+");
                List<Tuple2<String, Integer>> pairs = new ArrayList<>();
                for (String word : words) {
                    if (!stopWords.contains(word) && !word.isEmpty()) {
                        pairs.add(new Tuple2<>(word, 1));
                    }
                }
                return pairs.iterator();
            })
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)          // Swap key-value to sort by frequency
            .sortByKey(false)                 // Sort by frequency descending
            .mapToPair(Tuple2::swap);          // Swap back to original order

        // Display the top 20 most frequent words
        System.out.println("Top 20 Most Frequent Words in Titles:");
        wordCount
            .take(20)
            .forEach(word -> 
                System.out.println("Word: " + word._1 + ", Frequency: " + word._2)
            );

        
     // Task 6: Genre Diversity in Ratings
        System.out.println("\nTask 6: Genre Diversity in Ratings");

        // Extract (genre, rating) pairs
        JavaPairRDD<String, Double> genreRatings = moviesRDD
            .flatMapToPair(columns -> {
                String ratingStr = columns[5];
                double rating = Double.parseDouble(ratingStr);
                String[] genres = columns[4].split(", ");
                List<Tuple2<String, Double>> pairs = new ArrayList<>();
                for (String genre : genres) {
                    pairs.add(new Tuple2<>(genre, rating));
                }
                return pairs.iterator();
            });

        // Group ratings by genre
        JavaPairRDD<String, Iterable<Double>> groupedRatings = genreRatings.groupByKey();

        // Calculate standard deviation for each genre
        JavaPairRDD<String, Double> genreRatingStdDev = groupedRatings
            .mapValues(ratings -> {
                List<Double> ratingList = new ArrayList<>();
                ratings.forEach(ratingList::add);

                // Calculate Mean
                double mean = ratingList.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                // Calculate Variance
                double variance = ratingList.stream()
                        .mapToDouble(r -> Math.pow(r - mean, 2))
                        .average().orElse(0.0);

                // Standard Deviation
                return Math.sqrt(variance);
            });

        // Collect and Sort by Standard Deviation (Descending)
        List<Tuple2<String, Double>> sortedByStdDev = genreRatingStdDev
            .collect()
            .stream()
            .sorted((g1, g2) -> Double.compare(g2._2, g1._2)) // Descending order
            .collect(Collectors.toList());

        // Display the genres with the highest and lowest rating variability
        System.out.println("Genres with the Highest Rating Variability:");
        sortedByStdDev.stream().limit(3).forEach(genre -> 
            System.out.println(genre._1 + " - StdDev: " + genre._2)
        );

        System.out.println("\nGenres with the Lowest Rating Variability:");
        sortedByStdDev.stream().skip(Math.max(0, sortedByStdDev.size() - 3)).forEach(genre -> 
            System.out.println(genre._1 + " - StdDev: " + genre._2)
        );



        
        

     // Task 7: Certification Rating Distribution
        System.out.println("\nTask 7: Certification Rating Distribution");

        // Count number of movies for each certification type
        JavaPairRDD<String, Integer> certificationCount = moviesRDD
            .mapToPair(row -> new Tuple2<>(row[2], 1))
            .reduceByKey(Integer::sum);

        // Calculate average rating for each certification type
        JavaPairRDD<String, Double> certificationAvgRating = moviesRDD
            .mapToPair(row -> new Tuple2<>(row[2], new Tuple2<>(Double.parseDouble(row[5]), 1)))
            .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
            .mapValues(sumCount -> sumCount._1 / sumCount._2);

        // Identify the certification type with the highest average rating
        Tuple2<String, Double> maxAvgRating = certificationAvgRating
            .collect()
            .stream()
            .max(Comparator.comparingDouble(t -> t._2))
            .orElse(new Tuple2<>("None", 0.0)); // Default if no data

        // Display the number of movies by certification type
        System.out.println("Number of Movies by Certification:");
        certificationCount.collect().forEach(cert -> 
            System.out.println("Certification: " + cert._1 + ", Count: " + cert._2)
        );

        // Display the certification type with the highest average rating
        System.out.println("Certification Type with Highest Average Rating: " + maxAvgRating._1 + " (Avg. Rating: " + maxAvgRating._2 + ")");

      

     // Task 8: Comparing TV Shows and Movies
        System.out.println("\nTask 8: Comparing TV Shows and Movies");

     // Calculate Average Ratings and Votes for Movies
        JavaPairRDD<String, Tuple2<Double, Integer>> movieRatingsVotes = moviesRDD
                .mapToPair(fields -> new Tuple2<>(fields[1], 
                        new Tuple2<>(Double.parseDouble(fields[5]), Integer.parseInt(fields[8]))));

        // Calculate averages for movies
        Tuple2<Double, Integer> movieAverages = movieRatingsVotes
                .mapValues(tuple -> new Tuple2<>(tuple._1, 1)) // (rating, 1) for count
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)) // Sum ratings and count
                .mapValues(sumCount -> new Tuple2<>(sumCount._1 / sumCount._2, sumCount._2)) // Average rating and total votes
                .values()
                .reduce((a, b) -> new Tuple2<>((a._1 + b._1) / 2, (a._2 + b._2) / 2)); // Average of all years

        System.out.println("Movies -> Average Rating: " + movieAverages._1 + ", Average Votes: " + movieAverages._2);


        // Calculate Average Ratings and Votes for TV Shows
        JavaPairRDD<String, Tuple2<Double, Integer>> tvShowRatingsVotes = tvShowsRDD
                .mapToPair(fields -> new Tuple2<>(fields[1], 
                        new Tuple2<>(Double.parseDouble(fields[5]), Integer.parseInt(fields[8]))));

        // Calculate averages for TV shows
        Tuple2<Double, Integer> tvShowAverages = tvShowRatingsVotes
                .mapValues(tuple -> new Tuple2<>(tuple._1, 1)) // (rating, 1) for count
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)) // Sum ratings and count
                .mapValues(sumCount -> new Tuple2<>(sumCount._1 / sumCount._2, sumCount._2)) // Average rating and total votes
                .values()
                .reduce((a, b) -> new Tuple2<>((a._1 + b._1) / 2, (a._2 + b._2) / 2)); // Average of all years

        System.out.println("TV Shows -> Average Rating: " + tvShowAverages._1 + ", Average Votes: " + tvShowAverages._2);


        // Trend Analysis Over Time for Movies
        JavaPairRDD<String, Double> movieTrends = movieRatingsVotes
                .mapValues(tuple -> new Tuple2<>(tuple._1, 1)) // (rating, 1) for count
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)) // Sum ratings and count
                .mapValues(sumCount -> sumCount._1 / sumCount._2); // Average rating per year

        List<Tuple2<String, Double>> movieTrendList = movieTrends.collect();
        System.out.println("Movie Trends (Year -> Avg Rating):");
        movieTrendList.forEach(tuple -> System.out.println(tuple._1 + " -> " + tuple._2));


        // Trend Analysis Over Time for TV Shows
        JavaPairRDD<String, Double> tvShowTrends = tvShowRatingsVotes
                .mapValues(tuple -> new Tuple2<>(tuple._1, 1)) // (rating, 1) for count
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)) // Sum ratings and count
                .mapValues(sumCount -> sumCount._1 / sumCount._2); // Average rating per year

        List<Tuple2<String, Double>> tvShowTrendList = tvShowTrends.collect();
        System.out.println("TV Show Trends (Year -> Avg Rating):");
        tvShowTrendList.forEach(tuple -> System.out.println(tuple._1 + " -> " + tuple._2));

 
        


     // Task 9: Certification Rating Distribution
        System.out.println("\nTask 9: Certification Rating Distribution");

     // Group Movies by Certification
        JavaPairRDD<String, Tuple2<Double, Integer>> certificationRatings = moviesRDD
                .filter(fields -> !fields[2].isEmpty()) // Filter out movies with no certification
                .mapToPair(fields -> new Tuple2<>(fields[2], new Tuple2<>(Double.parseDouble(fields[5]), 1))); // (certification, (rating, 1))

        // Count Movies and Sum Ratings per Certification
        JavaPairRDD<String, Tuple2<Double, Integer>> certificationStats = certificationRatings
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)); // Sum ratings and count

        // Calculate Average Rating for Each Certification
        JavaPairRDD<String, Double> certificationAverages = certificationStats
                .mapValues(sumCount -> sumCount._1 / sumCount._2); // Average rating

        // Identify Certification with Highest Average Rating
        Tuple2<String, Double> highestAverage = certificationAverages
                .reduce((a, b) -> a._2 > b._2 ? a : b); // Get the max average

        // Display Results
        System.out.println("Certification Rating Distribution:");
        certificationStats.collect().forEach(cert -> 
            System.out.println("Certification: " + cert._1 + ", Number of Movies: " + cert._2._2 + ", Total Rating: " + cert._2._1)
        );

        System.out.println("\nAverage Ratings by Certification:");
        certificationAverages.collect().forEach(avg -> 
            System.out.println("Certification: " + avg._1 + ", Average Rating: " + avg._2)
        );

        System.out.println("\nHighest Average Rating:");
        System.out.println("Certification: " + highestAverage._1 + ", Average Rating: " + highestAverage._2);


        

    	

    	// Task 10: Comparing TV Shows and Movies
    	System.out.println("\nTask 10: Comparing TV Shows and Movies");

        
        JavaRDD<String[]> tvShowsRDD1 = yearCleaned.filter(fields -> fields[1].contains("–"));
        JavaRDD<String[]> moviesRDD1 = yearCleaned.filter(fields -> !fields[1].contains("–"));

        // Compute Average Rating and Total Votes for TV Shows
        // Extract (Rating, Votes) as (Double, Integer) pairs
        JavaPairRDD<Double, Integer> tvShowRatingsVotes1 = tvShowsRDD1.mapToPair(fields -> {
            double rating = fields[5].isEmpty() ? 0.0 : Double.parseDouble(fields[5]);
            String votesStr = fields[8].replaceAll("[^0-9]", ""); // Keep only numbers
            int votes = votesStr.isEmpty() ? 0 : Integer.parseInt(votesStr); // Convert to int or 0

            return new Tuple2<>(rating, votes);
        });

        // Calculate Total Ratings, Total Votes, and Count
        Tuple2<Double, Integer> tvShowAggregates = tvShowRatingsVotes1.reduce((a, b) -> 
            new Tuple2<>(a._1() + b._1(), a._2() + b._2())
        );
        long tvShowCount = tvShowsRDD1.count();
        double tvShowAvgRating = tvShowCount > 0 ? tvShowAggregates._1() / tvShowCount : 0.0;
        int tvShowTotalVotes = tvShowAggregates._2();

        // Compute Average Rating and Total Votes for Movies
        JavaPairRDD<Double, Integer> movieRatingsVotes1 = moviesRDD1.mapToPair(fields -> {
            double rating = fields[5].isEmpty() ? 0.0 : Double.parseDouble(fields[5]);
            String votesStr = fields[8].replaceAll("[^0-9]", ""); // Keep only numbers
            int votes = votesStr.isEmpty() ? 0 : Integer.parseInt(votesStr); // Convert to int or 0
            return new Tuple2<>(rating, votes);
        });

        Tuple2<Double, Integer> movieAggregates = movieRatingsVotes1.reduce((a, b) -> 
            new Tuple2<>(a._1() + b._1(), a._2() + b._2())
        );
        long movieCount = moviesRDD1.count();
        double movieAvgRating = movieCount > 0 ? movieAggregates._1() / movieCount : 0.0;
        int movieTotalVotes = movieAggregates._2();

        // Analyze Total Votes Per Year for TV Shows
        JavaPairRDD<String, Integer> tvShowVotesPerYear = tvShowsRDD1
        		.mapToPair(fields -> {
        		    String votesStr = fields[8].replaceAll("[^0-9]", ""); // Keep only numbers
        		    int votes = votesStr.isEmpty() ? 0 : Integer.parseInt(votesStr); // Convert to int or 0
        		    return new Tuple2<>(fields[1], votes);
        		})
            .reduceByKey(Integer::sum)
            .sortByKey();

        // Analyze Total Votes Per Year for Movies
        JavaPairRDD<String, Integer> movieVotesPerYear = moviesRDD1
            .mapToPair(fields -> new Tuple2<>(fields[1], Integer.parseInt(fields[8])))
            .reduceByKey(Integer::sum)
            .sortByKey();

        // Display Results
        System.out.println("=== TV Shows Analysis ===");
        System.out.println("Average Rating: " + tvShowAvgRating);
        System.out.println("Total Votes: " + tvShowTotalVotes);
        System.out.println("Votes Per Year:");
        tvShowVotesPerYear.collect().forEach(entry -> 
            System.out.println("Year: " + entry._1() + ", Votes: " + entry._2())
        );

        System.out.println("\n=== Movies Analysis ===");
        System.out.println("Average Rating: " + movieAvgRating);
        System.out.println("Total Votes: " + movieTotalVotes);
        System.out.println("Votes Per Year:");
        movieVotesPerYear.collect().forEach(entry -> 
            System.out.println("Year: " + entry._1() + ", Votes: " + entry._2())
        );












        
        // Stop Spark Context
        sc.stop();
    }
}
