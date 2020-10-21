package RDDAssignment

import java.util.UUID

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
   * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
   * we want to know how many commits a given RDD contains.
   *
   * @param commits RDD containing commit data.
   * @return Long indicating the number of commits in the given RDD.
   */
  def assignment_1(commits: RDD[Commit]): Long = {
    commits.collect().length
  }

  /**
   * We want to know how often programming languages are used in committed files. We require a RDD containing Tuples
   * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
   * assume the language to be 'unknown'.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
   */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {
    commits.flatMap(commit => commit.files.flatMap(fileOption => fileOption.filename))
      .filter(x => x.lastIndexOf(".") != -1)
      .map(x => x.substring(x.lastIndexOf(".") + 1))
      .groupBy(x => x).map(x => (x._1, x._2.size))
  }

  /**
   * Competitive users on Github might be interested in their ranking in number of commits. We require as return a
   * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit authors name and the number of
   * commits made by the commit author. As in general with performance rankings, a higher performance means a better
   * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
   * tie.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing commit author names and total count of commits done by the author, in ordered fashion.
   */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {
    commits.map(commit => commit.commit.author.name)
      .groupBy(x => x).map(x => (x._1, x._2.size)).sortBy(r => (-r._2, r._1.toLowerCase()))
      .zipWithIndex().map(x => (x._2, x._1._1, x._1._2))
  }

  /**
   * Some users are interested in seeing an overall contribution of all their work. For this exercise we an RDD that
   * contains the committer name and the total of their commits. As stats are optional, missing Stat cases should be
   * handles as s"Stat(0, 0, 0)". If an User is given that is not in the dataset, then the username should not occur in
   * the return RDD.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing committer names and an aggregation of the committers Stats.
   */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    commits.map(c => (c.commit.committer.name, c.stats.getOrElse(Stats(0, 0, 0))))
      .groupBy(_._1).map(x => (x._1, x._2.map(x => x._2)))
      .filter(c => users.contains(c._1))
      .map(x => (x._1, x._2.foldLeft(Stats(0, 0, 0))((a, b) => Stats(a.total + b.total, a.additions + b.additions, a.deletions + b.deletions))))
  }


  /**
   * There are different types of people, those who own repositories, and those who make commits. Although Git blame is
   * excellent in finding these types of people, we want to do it in Spark. We require as output an RDD containing the
   * names of commit authors and repository owners that have either exclusively committed to repositories, or
   * exclusively own repositories in the given RDD. Note that the repository owner is contained within Github urls.
   *
   * @param commits RDD containing commit data.
   * @return RDD of Strings representing the username that have either only committed to repositories or only own
   *         repositories.
   */
  def assignment_5(commits: RDD[Commit]): RDD[String] = {

    commits.flatMap(c => Array((c.url.substring(c.url.indexOf("repos/") + 6, c.url.indexOf("/",
      c.url.indexOf("repos/") + 7)), 0, 1), (c.commit.committer.name, 1, 0)))
      .groupBy(_._1).map(x => (x._1, x._2.foldLeft((0, 0))((b, i) => (b._1 + i._2, b._2 + i._3))))
      .filter(c => (c._2._1 == 0 || c._2._2 == 0)).map(x => x._1)
  }


  /**
   * Sometimes developers make mistakes, sometimes they make many. One way of observing mistakes in commits is by
   * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
   * in a commit. Note that for a commit to be eligible for a 'commit streak', its message must start with `Revert`.
   * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
   * would not be a 'revert streak' at all.
   * We require as return a RDD containing Tuples of the username of a commit author and a Tuple containing
   * the length of the longest streak of an user and how often said streak has occurred.
   * Note that we are only interested in the longest commit streak of each author (and its frequency).
   *
   * @param commits RDD containing commit data.
   * @return RDD of Tuple type containing a commit author username, and a tuple containing the length of the longest
   *         commit streak as well its frequency.
   */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {
    commits.map(c => (c.commit.author.name, c.commit.message)).filter(m => m._2.startsWith("Revert"))
      .map(c => (c._1, "(Revert)".r.findAllMatchIn(c._2).size)).groupBy(_._1)
      .map(x => (x._1, x._2.foldLeft((0, 0))((b, inst) => if (inst._2 > b._1) (inst._2, 1)
      else if (inst._2 == b._1) (b._1, b._2 + 1) else b)))
  }


  def extract_repo_name(commit: Commit): String = {
    val beginIndex = commit.url.indexOf("/", commit.url.indexOf("repos/") + 7)
    commit.url.substring(beginIndex + 1, commit.url.indexOf("/", beginIndex + 1))
  }

  /**
   * We want to know the number of commits that are made to each repository contained in the given RDD. Besides the
   * number of commits, we also want to know the unique committers that contributed to the repository. Note that from
   * this exercise on, expensive functions like groupBy are no longer allowed to be used. In real life these wide
   * dependency functions are performance killers, but luckily there are better performing alternatives!
   * The automatic graders will check the computation history of the returned RDDs.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing a tuple indicating the repository name, the number of commits made to the repository as
   *         well as the unique committer usernames that committed to the repository.
   */


  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {
    commits.map(c => (extract_repo_name(c), (1, Set(c.commit.committer.name))))
      .reduceByKey((acc, x) => (acc._1 + x._1, acc._2 ++ x._2))
      .map(x => (x._1, x._2._1, x._2._2))
  }

  /**
   * Return RDD of tuples containing the repository name and all the files that are contained in that repository.
   * Note that the file names must be unique, so if files occur multiple times (for example due to removal, or new
   * additions), the newest File object must be returned. As the files' filenames are an `Option[String]` discard the
   * files that do not have a filename.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the files in each repository as described above.
   */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {
    val extractedRepoNames = commits.map(c => (extract_repo_name(c), c.files))
    val repositoriesWithFiles = extractedRepoNames.reduceByKey((acc, x) => acc ++ x)
    // Now all repositories contain their full list of files
    val mapToContainBoth = extractedRepoNames.map(x => (x._1, x._2.map(x => (x.filename, x))))
    val filterOutNones = mapToContainBoth.map(x => (x._1, x._2.filter(y => y._1.isDefined).map(y => (y._1.get, y._2))))
    val filesReduceByKey = filterOutNones.map(x => (x._1, x._2.foldLeft(Map[String, File]()) { (acc, x) => acc.updated(x._1, x._2) }))
    filesReduceByKey.map(x => (x._1, x._2.values.toList.reverse))
  }


  /**
   * For this assignment you are asked to find all the files of a single repository. This in order to create an
   * overview of each files, do this by creating a tuple containing the file name, all corresponding commit SHAs
   * as well as a Stat object representing the changes made to the file.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples representing a file name, its corresponding commit SHAs and a Stats object
   *         representing the total aggregation of changes for a file.
   */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {
    commits.filter(c => extract_repo_name(c) == repository)
      .flatMap(c => c.files.map(x => (x.filename.get, (List(x.sha.get), Stats(x.changes, x.additions, x.deletions)))))
      .reduceByKey((acc, x) => (acc._1 ++ x._1, Stats(acc._2.total + x._2.total, acc._2.additions + x._2.additions,
        acc._2.deletions + x._2.deletions))).map(k => (k._1, k._2._1, k._2._2))
  }

  /**
   * We want to generate an overview of the work done by an user per repository. For this we request an RDD containing a
   * tuple containing the committer username, repository name and a `Stats` object. The Stats object containing the
   * total number of additions, deletions and total contribution.
   * Note that as Stats are optional, therefore a type of Option[Stat] is required.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing tuples of committer names, repository names and and Option[Stat] representing additions and
   *         deletions.
   */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {
    commits.map(x => ((extract_repo_name(x), x.commit.committer.name), x.stats)).reduceByKey((acc, x) => if (x.isDefined && acc.isDefined) Option(Stats(x.get.total + acc.get.total, x.get.additions + acc.get.additions, x.get.deletions + acc.get.deletions)) else if (acc.isEmpty) x else acc).map(x => (x._1._2, x._1._1, x._2))
  }


  /**
   * Hashing function that computes the md5 hash from a String, which in terms returns a Long to act as a hashing
   * function for repository name and username.
   *
   * @param s String to be hashed, consecutively mapped to a Long.
   * @return Long representing the MSB from the inputted String.
   */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
   * Create a bi-directional graph from committer to repositories, use the md5HashString function above to create unique
   * identifiers for the creation of the graph. This exercise is meant as an extra, and is not mandatory to complete.
   * As the real usage Sparks GraphX library is out of the scope of this course, we will not go further into this, but
   * this can be used for algorithms like PageRank, Hubs and Authorities, clique finding, ect.
   *
   * We expect a node for each repository and each committer (based on committer name). We expect an edge from each
   * committer to the repositories that the developer has committed to.
   *
   * Look into the documentation of Graph and Edge before starting with this (complementary) exercise.
   * Your vertices should contain information about the type of node, a 'developer' or a 'repository' node.
   * Edges should only exist between repositories and committers.
   *
   * @param commits RDD containing commit data.
   * @return Graph representation of the commits as described above.
   */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = ???
}
