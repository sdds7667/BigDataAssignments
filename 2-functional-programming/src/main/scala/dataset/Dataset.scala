package dataset

import java.text.SimpleDateFormat
import java.util.TimeZone

import dataset.util.Commit.Commit

/**
 * Use your knowledge of functional programming to complete the following functions.
 * You are recommended to use library functions when possible.
 *
 * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
 * When asked for dates, use the `commit.commit.committer.date` field.
 *
 * This part is worth 40 points.
 */
object Dataset {

  /** Q16 (5p)
   * For the commits that are accompanied with stats data, compute the average of their additions.
   * You can assume a positive amount of usable commits is present in the data.
   *
   * @param input the list of commits to process.
   * @return the average amount of additions in the commits that have stats data.
   */
  def avgAdditions(input: List[Commit]): Int = {
    val xs = input.flatMap(x => x.stats).map(x => x.additions)
    xs.sum / xs.length
  }

  /** Q17 (8p)
   * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
   * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   *
   * @param input list of commits to process.
   * @return the hour and the amount of files changed during this hour.
   */
  def jsTime(input: List[Commit]): (Int, Int) = {
    val timeFormat = new SimpleDateFormat("HH")
    timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    input.map(x => (timeFormat.format(x.commit.committer.date), x.files.flatMap(x => x.filename).count(x => x.endsWith(".js"))))
      .groupBy(_._1).map(x => (x._1.toInt, x._2.aggregate(0)((b, x) => x._2 + b, (a, b) => a + b)))
      .maxBy(_._2)
  }

  /** Q18 (9p)
   * For a given repository, output the name and amount of commits for the person
   * with the most commits to this repository.
   * For the name, use `commit.commit.author.name`.
   *
   * @param input the list of commits to process.
   * @param repo  the repository name to consider.
   * @return the name and amount of commits for the top committer.
   */
  def topCommitter(input: List[Commit], repo: String): (String, Int) =
    input.filter(x => x.url.contains(repo)).map(y => y.commit.author.name)
      .groupBy(identity).map(z => (z._1, z._2.length)).toSeq.sortBy(_._2).reverse.head

  /** Q19 (9p)
   * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
   * Leave out all repositories that had no activity this year.
   *
   * @param input the list of commits to process.
   * @return a map that maps the repo name to the amount of commits.
   *
   *         Example output:
   *         Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
   */
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
    val yearExtractor = new SimpleDateFormat("yyyy")
    input
      .map(x => ("""repos.*commits""".r.findFirstIn(x.url).get, if (yearExtractor.format(x.commit.author.date) == "2019") {
        1
      } else {
        0
      }))
      .map(x => (x._1.substring(6, x._1.length - 8), x._2))
      .groupBy(_._1).map(x => (x._1, x._2.aggregate(0)((b, a) => b + a._2, (b, c) => b + c)))
  }

  /** Q20 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   *
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = {
    val reg = """([a-zA-Z0-9\-]+)$""".r
    input.flatMap(x => x.files.flatMap(y => y.filename)).flatMap(y => reg.findAllIn(y)).groupBy(x => x)
      .map(z => (z._1, z._2.length)).toSeq.sortBy(_._2).reverse.take(5).toList
  }
}
