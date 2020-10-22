package DataFrameAssignment

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Note read the comments carefully, as they describe the expected result and may contain hints in how
 * to tackle the exercises. Note that the data that is given in the examples in the comments does
 * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
 */
object DFAssignment {

  /**
   * In this exercise we want to know all the commit SHA's from a list of commit committers. We require these to be
   * in order according to timestamp.
   *
   * | committer      | sha                                      | timestamp            |
   * |----------------|------------------------------------------|----------------------|
   * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
   * | ...            | ...                                      | ...                  |
   *
   * Hint: try to work out the individual stages of the exercises, which makes it easier to track bugs, and figure out
   * how Spark Dataframes and their operations work. You can also use the `printSchema()` function and `show()`
   * function to take a look at the structure and contents of the Dataframes.
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @param authors Sequence of String representing the authors from which we want to know their respective commit
   *                SHA's.
   * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
   */
  def assignment_1(commits: DataFrame, authors: Seq[String]): DataFrame = {
    val x = commits.withColumn("committer", commits.col("commit.committer.name"))
      .withColumn("timestamp", commits.col("commit.committer.date"))
      .select("committer", "sha", "timestamp")
      .filter(commits.col("commit.committer.name").isin(authors: _*))
      .orderBy(asc("timestamp"))

    x.show()
    x
  }

  /**
   * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
   * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
   * The returned DataFrame that is expected is in the following format:
   *
   * | repository | week             | year | count   |
   * |------------|------------------|------|---------|
   * | Maven      | 41               | 2019 | 21      |
   * | .....      | ..               | .... | ..      |
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @return Dataframe containing 4 columns, Repository name, week number, year and the number fo commits for that
   *         week.
   */
  def assignment_2(commits: DataFrame): DataFrame = {
    commits.printSchema()
    val window = Window.partitionBy("repository", "year")
      .orderBy("repository", "year")

    val extract_repo_name: String => String = fExtractRepoName(_)
    val repo_udf = udf(extract_repo_name)

    val x = commits.withColumn("new_date", to_timestamp(col("commit.author.date")))
      .withColumn("year", year(col("new_date")))
      .withColumn("week", weekofyear(col("new_date")))
      .withColumn("repository", repo_udf(col("url")))
      .withColumn("count", count(col("commit.committer.date")).over(window))
      .select("repository", "week", "year", "count")
      .dropDuplicates()
    x.show()
    x
  }

  /**
   * A developer is interested in the age of commits in seconds, although this is something that can always be
   * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore we
   * require you to append the inputted DataFrame with an age column of each commit in `seconds`.
   *
   * Hint: Look into SQL functions in for Spark SQL.
   *
   * Expected Dataframe (column) example that is expected:
   *
   * | age    |
   * |--------|
   * | 1231   |
   * | 20     |
   * | ...    |
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @return the inputted DataFrame appended with an age column.
   */
  def assignment_3(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {
    commits.printSchema()
    // snapShotTimestamp = 2019-10-10 13:30:29.0
    val x = commits
      .withColumn("age", (lit(snapShotTimestamp)).cast(LongType) - to_timestamp(col("commit.committer.date")).cast(LongType))
    x.show()
    x
  }

  /**
   * To perform analysis on commit behavior the intermediate time of commits is needed. We require that the DataFrame
   * that is put in is appended with an extra column that expresses the number of days there are between the current
   * commit and the previous commit of the user, independent of the branch or repository.
   * If no commit exists before a commit regard the time difference in days should be zero. Make sure to return the
   * commits in chronological order.
   *
   * Hint: Look into Spark sql's Window to have more expressive power in custom aggregations
   *
   * Expected Dataframe example:
   *
   * | $oid                     	| name   	| date                     	| time_diff 	|
   * |--------------------------	|--------	|--------------------------	|-----------	|
   * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
   * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
   * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
   * | ...                      	| ...    	| ...                      	| ...       	|
   *
   * @param commits    Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                   `println(commits.schema)`.
   * @param authorName Name of the author for which the result must be generated.
   * @return DataFrame with column expressing days since last commit.
   */
  def assignment_4(commits: DataFrame, authorName: String): DataFrame = {
    commits.printSchema()
    val window = Window.partitionBy("commit.committer.name")
      .orderBy(asc("commit.committer.date"))

    val x = commits
      .withColumn("prev", to_timestamp(lag("commit.committer.date", 1).over(window)))
      .withColumn("new_date", to_timestamp(col("commit.committer.date")))
      .withColumn("time_diff", round((col("new_date").cast(LongType) - col("prev").cast(LongType)) / 86400).cast(IntegerType))
      .select("_id.$oid", "commit.committer.name", "commit.committer.date", "time_diff")
      .filter(col("commit.committer.name").equalTo(authorName))
      .orderBy(asc("commit.committer.date"))
      .na.fill(0)

    x.show()
    x
  }


  /**
   * To get a bit of insight in the spark SQL, and its aggregation functions, you will have to implement a function
   * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits
   * commit date. Sunday would be 1, Monday 2, etc.
   *
   * Expected Dataframe example:
   *
   * | day | commits_per_day|
   * |-----|----------------|
   * | 0   | 32             |
   * | ... | ...            |
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @return DataFrame containing a `day` column and a `commits_per_day` representing a count of the total number of
   *         commits that that were ever made on that week day.
   */
  def assignment_5(commits: DataFrame): DataFrame = {
    //Expected Set((7,7), (4,71), (3,34), (1,9), (2,31), (5,1816), (6,20)),
    // but got Set((3,35), (7,6), (4,68), (1,10), (6,21), (5,1818), (2,30))
    commits.printSchema()
    val window = Window.partitionBy("day")
      .orderBy(asc("day"))

    val x = commits
      .withColumn("new_date", to_timestamp(col("commit.committer.date")))
      .withColumn("day", dayofweek(col("new_date")))
      .withColumn("commits_per_day", count(col("day")).over(window))
      .select("day", "commits_per_day")
      .orderBy(asc("day"))
      .na.fill(0)

    x.show()
    x
  }

  /**
   * Commits can be uploaded on different days, we want to get insight in difference in commit time of the author and
   * the committer. Append the given dataframe with a column expressing the number of seconds in difference between
   * the two events in the commit data.
   *
   * Expected Dataframe (column) example:
   *
   * | commit_time_diff |
   * |------------------|
   * | 1022             |
   * | 0                |
   * | ...              |
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @return original Dataframe appended with a column `commit_time_diff` containing the number of seconds time
   *         difference between authorizing and committing.
   */
  def assignment_6(commits: DataFrame): DataFrame = {
    commits.printSchema()
    val x = commits
      .withColumn("auth", to_timestamp(col("commit.author.date")))
      .withColumn("com", to_timestamp(col("commit.committer.date")))
      .withColumn("diff", col("com").cast(LongType) - col("auth").cast(LongType))
      .withColumn("commit_time_diff", col("diff"))
      .select("commit_time_diff")

    x.show()
    x
    //Cannot resolve column name "auth" among (_id, author, commit, committer, files, node_id, parents, sha, stats, url);
  }

  /**
   * Using Dataframes find all the commit SHA's from which a branch was created, including the number of
   * branches that were made. Only take the SHA's into account if they are also contained in the DataFrame.
   * Note that the returned Dataframe should not contain any SHA's of which no new branches were made, and should not
   * contain a SHA which is not contained in the given Dataframe.
   *
   * Expected Dataframe example:
   *
   * | sha                                      | times_parent |
   * |------------------------------------------|--------------|
   * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
   * | ...                                      | ...          |
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @return DataFrame containing the SHAs of which a new branch was made.
   */
  def assignment_7(commits: DataFrame): DataFrame = {
    // commit.tree.sha
    // what branch?
    commits.printSchema()
    val x = commits
      //      .withColumn("auth", to_timestamp(commits.col("commit.author.date")))
      .select("commit.tree.*")
    //      .filter(commits.col("commit.tree.sha").equalTo(commits.rdd))

    x.show()
    x
  }

  /**
   * Find of commits from which a fork was created in the given commit DataFrame. We are interested in the name of
   * repositories, the parent and the subsequent fork, including the name of the repository owner. The SHA from which
   * the fork was created (parent_sha) as well as the first SHA that occurs in the forked branch (child_sha).
   *
   * Expected Dataframe example:
   *
   * | repo_name            | child_repo_name     | parent_sha           | child_sha            |
   * |----------------------|---------------------|----------------------|----------------------|
   * | ElucidataInc/ElMaven | saifulbkhan/ElMaven | 37d38cb21ab342b17... | 6a3dbead35c10add6... |
   * | hyho942/hecoco       | Sub2n/hecoco        | ebd077a028bd2169d... | b47db8a9df414e28b... |
   * | ...                  | ...                 | ...                  | ...                  |
   *
   * Note that this example is based on _real_ data, so you can verify the functionality of your solution, which might
   * help during debugging your solution.
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @return DataFrame containing the SHAs of which a new fork was created.
   */
  def assignment_8(commits: DataFrame): DataFrame = {
    val extract_repo_name: String => String = fExtractRepoName(_)
    val repo_udf = udf(extract_repo_name)

    val extract_user_name: String => String = fExtractUserName(_)
    val user_udf = udf(extract_user_name)
    val x = commits.select("url", "commit.tree", "parents")
      .withColumn("repoName", repo_udf(col("url")))
      .withColumn("user", user_udf(col("url")))
      .withColumn("int_sha", element_at(col("parents"), 1))
      .filter(col("url").contains("nantesjs-website"))
    x.show()
    x
  }

  def fExtractUserName(url: String): String = {
    val beginIndex = url.indexOf("repos/") + 7
    url.substring(beginIndex, url.indexOf("/", beginIndex + 1))
  }

  def fExtractRepoName(url: String): String = {
    val beginIndex = url.indexOf("/", url.indexOf("repos/") + 7)
    url.substring(beginIndex + 1, url.indexOf("/", beginIndex + 1))
  }
}