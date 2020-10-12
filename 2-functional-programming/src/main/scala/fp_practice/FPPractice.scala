package fp_practice

/**
 * In this part you can practice your FP skills with some small exercises.
 * Hint: you can find many useful functions in the documentation of the List class.
 *
 * This part is worth 15 points.
 */
object FPPractice {

  /** Q13 (4p)
   * Returns the sum of the first 10 numbers larger than 25 in the given list.
   *
   * @param xs the list to process.
   * @return the sum of the first 10 numbers larger than 25.
   */
  def first10Above25(xs: List[Int]): Int = xs.filter(_ > 25).take(10).sum

  /** Q14 (5p)
   * Provided with a list of all grades for each student of a course,
   * count the amount of passing students.
   * A student passes the course when the average of the grades is at least 5.75 and no grade is lower than 4.
   *
   * @param grades a list containing a list of grades for each student.
   * @return the amount of students with passing grades.
   */
  def passingStudents(grades: List[List[Int]]): Int = grades.filter(_.min >= 4).map((x: List[Int]) => x.sum.toFloat / x.length).count(_ > 5.75)

  /** Q15 (6p)
   * Return the length of the first list of which the first item's value is equal to the sum of all other items.
   *
   * @param xs the list to process
   * @return the length of the first list of which the first item's value is equal to the sum of all other items,
   *         or None if no such list exists.
   *
   *         Read the documentation on the `Option` class to find out what you should return.
   *         Hint: it is very similar to the `OptionalInt` you saw earlier.
   */
  def headSumsTail(xs: List[List[Int]]): Option[Int] =
    xs.map((x: List[Int]) => if (x.nonEmpty) if (x.head == x.tail.sum) x.length else -1 else -1).find(_ > 0)


}
