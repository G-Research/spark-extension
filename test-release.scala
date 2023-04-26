import uk.co.gresearch.spark.diff._

val left = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
val right = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")

val diff = left.diff(right)
diff.show()

if (diff.collect().size == 5) { sys.exit(0) }

sys.exit(1)

