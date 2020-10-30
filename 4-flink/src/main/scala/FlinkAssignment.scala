import java.util
import java.util.Date

import _root_.util.Protocol._
import _root_.util.{CommitGeoParser, CommitParser}
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.shaded.org.joda.time.DateTime
import org.apache.flink.util.Collector

/** Do NOT rename this class, otherwise autograding will fail. * */
object FlinkAssignment {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
     * Setups the streaming environment including loading and parsing of the datasets.
     *
     * DO NOT TOUCH!
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    // dummy_question(commitStream).print()
    // question_one(commitStream).print()
    //question_three(commitStream).print()
    //question_nine(commitStream).print()
    question_six(commitStream).print()


    /** Start the streaming environment. * */
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
//  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
//    input.map(_.sha)
//  }

  /**
   * Write a Flink application which outputs the sha of commits with at least 20 additions.
   * Output format: sha
   */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input.map(x => (x.sha, x.stats.getOrElse(Stats(0, 0, 0))))
      .filter(x => x._2.additions >= 20)
      .map(x => x._1)
  }

  /**
   * Write a Flink application which outputs the names of the files with more than 30 deletions.
   * Output format:  fileName
   */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input.flatMap(x => x.files.filter(y => y.filename.isDefined)
      .map(y => (y.filename.get, y.deletions))).filter(x => x._2 > 30)
      .map(x => x._1)
  }

  /**
   * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
   * Output format: (fileExtension, #occurrences)
   */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(x => x.files.filter(y => y.filename.isDefined)
      .map(y => y.filename.get)).filter(z => (z.endsWith(".scala") || z.endsWith(".java")))
      .map(z => (z.substring(z.lastIndexOf(".") + 1), 1)).keyBy(x => x._1).reduce((a, b) => (a._1, a._2 + b._2))
  }

  /**
   * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
   * Output format: (extension, status, count)
   */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] =
    input.flatMap(x => x.files.filter(y => y.filename.isDefined)
      .map(y => (y.filename.get, y.status.get))).filter(z => (z._1.endsWith(".js") || z._1.endsWith(".py")))
    .map(z => (z._1.substring(z._1.lastIndexOf(".") + 1), z._2, 1))
    .keyBy(x => x._1)
    //    .keyBy(y => y._2)
    //    .sum(2)
    .reduce((a, b) => (a._1, a._2, a._3 + b._3))


  /**
   * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
   * Make use of a non-keyed window.
   * Output format: (date, count)
   */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {

    input.map(c => (new DateTime(c.commit.committer.date).toString("dd-MM-yyyy"), 1, c.commit.committer.date))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Int, Date)] {
        override def getCurrentWatermark: Watermark = {
          new Watermark(0)
        }

        override def extractTimestamp(element: (String, Int, Date), previousElementTimestamp: Long): Long = {
          element._3.getTime
        }
      }).windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .reduce((a, b) => (a._1, a._2 + b._2, a._3))
      .map(x => (x._1, x._2))
  }

  /**
   * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
   * Compute every 12 hours the amount of small and large commits in the last 48 hours.
   * Output format: (type, count)
   */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] =
  input
    .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Commit] {
      override def getCurrentWatermark: Watermark = new Watermark(0)

      override def extractTimestamp(t: Commit, l: Long): Long = t.commit.committer.date.getTime
    })
    .map(x => x.stats.get.total)
    .map(z => (z, 1))
    .keyBy(x => x._1)
    .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
    .reduce((a, b) => (a._1, a._2 + b._2))
    //      .sum(1)
    .map(s => if (s._2 > 20) ("large", s._2) else ("small", s._2))


  /**
   * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
   *
   * The fields of this case class:
   *
   * repo: name of the repo.
   * date: use the start of the window in format "dd-MM-yyyy".
   * amountOfCommits: the number of commits on that day for that repository.
   * amountOfCommitters: the amount of unique committers contributing to the repository.
   * totalChanges: the sum of total changes in all commits.
   * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
   *
   * Hint: Write your own ProcessWindowFunction.
   * Output format: CommitSummary
   */
  def repo_name(url: String): String = {
    val beginIndex = url.indexOf("/", url.indexOf("repos/") + 7)
    url.substring(beginIndex + 1, url.indexOf("/", beginIndex + 1))
  }

  def question_seven(commitStream: DataStream[Commit]):
  DataStream[CommitSummary] = {
    val assigner = new AssignerWithPeriodicWatermarks[(String, String, Int, Date)] {
      override def getCurrentWatermark: Watermark = {
        new Watermark(0);
      }

      override def extractTimestamp(element: (String, String, Int, Date), previousElementTimestamp: Long): Long = {
        element._4.getTime
      }
    }

    class ProcessDays extends ProcessAllWindowFunction[(String, String, Int, Date), CommitSummary, TimeWindow] {
      override def process(context: Context, elements: Iterable[(String, String, Int, Date)], out: Collector[CommitSummary]): Unit = {
        if (elements.size >= 20) {
          None
        } else {
          val repoName = elements.head._1
          val totalCommits = elements.size
          val date = new DateTime(elements.head._4).toString("dd-MM-yyyy")
          val committers = elements.groupBy(_._2)
          val totalCommitters = committers.size

          if (committers.keys.size > 2) {
            None
          } else {
            val topCommitterCount = committers.map(x => (x._1, x._2.foldLeft(0)((b, a) => b + a._3)))
              .toSeq.sortBy(_._2).reverse
            val totalChanges = topCommitterCount.foldLeft(0)((b, a) => b + a._2)
            val topCommitter = topCommitterCount.filter(x => x._2 != topCommitterCount.head._2)
              .foldLeft("")((b, a) => b + "," + a._1)
            out.collect(CommitSummary(repoName, date, totalCommits, totalCommitters, totalChanges, if (topCommitter.size > 1) topCommitter.substring(1) else topCommitter))
          }
        }
      }
    }

    commitStream.filter(x => x.stats.isDefined)
      .map(x => (repo_name(x.url), x.commit.committer.name, x.stats.get.total, x.commit.committer.date))
      .keyBy(_._1)
      .assignTimestampsAndWatermarks(assigner)
      .timeWindowAll(Time.days(1))
      .process(new ProcessDays())

  }

  /**
   * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
   * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
   * Get the weekly amount of changes for the java files (.java extension) per continent.
   *
   * Hint: Find the correct join to use!
   * Output format: (continent, amount)
   */
  def question_eight(commitStream: DataStream[Commit],
                      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = ???
//    .map(z => (z, 1))
//    .keyBy(x => x._1)
//    .timeWindow(Time.hours(48), Time.hours(12))
//    .sum(2)
//    .map(s => if (s._2 > 20) ("large", s._2) else ("small", s._2))

  /**
   * Find all files that were added and removed within one day. Output as (repository, filename).
   *
   * Hint: Use the Complex Event Processing library (CEP).
   * Output format: (repository, filename)
   */
  def question_nine(inputStream: DataStream[Commit]):
  DataStream[(String, String)] = {
    val pattern = Pattern.begin[(File, (String, Date))]("start")
      .where(_._1.status.get == "added")

    val files_urls = inputStream.flatMap(x => x.files.zip(Stream.continually((x.url, x.commit.committer.date))))
      .filter(x => x._1.status.isDefined)
    val patternStream = CEP.pattern(files_urls, pattern)
    patternStream.process(
      new PatternProcessFunction[(File, (String, Date)), (String, String)] {
        override def processMatch(`match`: util.Map[String, util.List[(File, (String, Date))]], ctx: PatternProcessFunction.Context, out: Collector[(String, String)]): Unit = {
          print("Found match!")
          val results = `match`;
          out.collect((repo_name(`match`.get().get(0)._2._1), `match`.get().get(0)._1.filename.get))
          //          if (new DateTime((`match`.get().get(0)._2._2)).withTimeAtStartOfDay()
          //            .isEqual(new DateTime(`match`.get().get(0)._2._2).withTimeAtStartOfDay()) &&
          //            `match`.get().get(0)._1.filename.get == `match`.get().get(1)._1.filename.get) {
          //            out.collect((repo_name(`match`.get().get(0)._2._1), `match`.get().get(0)._1.filename.get))
          //          }
        }
      })
  }
}