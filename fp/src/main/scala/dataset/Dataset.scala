package dataset

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Date, SimpleTimeZone}

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
      * @param input list of commits to process.
      * @return the hour and the amount of files changed during this hour.
      */
    def jsTime(input: List[Commit]) = input.map(x => (new SimpleDateFormat("HH").format(x.commit.committer.date),
      new SimpleDateFormat("HH").format(x.commit.author.date)))

    //    cat 1000_commits.json | jq '.' -s | jq '. | group_by(.commit.author.date | strptime("%Y-%m-%dT%H:%M:%S%Z") | .[3]) | .[] |
    //    map(.files[].filename | select(endswith(".js"))) | length' | sort -nk 1 | tail -n 1


    //.map(x => (new SimpleDateFormat("HH").format(x.commit.committer.date).toInt,
      //  x.files.flatMap(y => y.filename).count(_.endsWith(".js")))).groupBy(_._1)
      //.mapValues(_.map(_._2).sum)
      //.map(x=>(x._1,x._2.map(y => y._2).sum)).maxBy(_._2)
    /** Q18 (9p)
      * For a given repository, output the name and amount of commits for the person
      * with the most commits to this repository.
      * For the name, use `commit.commit.author.name`.
      * @param input the list of commits to process.
      * @param repo the repository name to consider.
      * @return the name and amount of commits for the top committer.
      */
    def topCommitter(input: List[Commit], repo: String): (String, Int) = ???
//        val name = input.sortBy(_.stats.map(x => x.total)).take(1)
//          .sortBy(_.stats.map(x => x.total)).take(1)
//          .map(commits => commits.author.name

    /** Q19 (9p)
      * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
      * Leave out all repositories that had no activity this year.
      * @param input the list of commits to process.
      * @return a map that maps the repo name to the amount of commits.
      *
      * Example output:
      * Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
      */
    def commitsPerRepo(input: List[Commit]): Map[String, Int] = ???

    /** Q20 (9p)
      * Derive the 5 file types that appear most frequent in the commit logs.
      * @param input the list of commits to process.
      * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
      */
    def topFileFormats(input: List[Commit]): List[(String, Int)] = ???
//        print(input.map(x => x.files.flatMap(y => y.filename)
//          .filter(_.matches("(*[.]*)"))))
//
//        val a: List[(String, Int)] = ("a",1):: Nil
//        a
//    }
//        .endsWith(".js")))).groupBy(_._1))
}
