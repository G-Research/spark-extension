package uk.co.gresearch.spark.group

import org.scalatest.funspec.AnyFunSpec

class GroupSuite extends AnyFunSpec {

  describe("GroupedIterator") {

    describe("should work with empty iterator") {
      test[Int, Double](() => Iterator.empty)
    }

    describe("should work with null key") {
      test(() => Iterator((null, 1.0), (null, 2.0), ("2", 3.0), ("2", 4.0), ("3", 5.0)))
    }

    describe("should work with None key") {
      test(() => Iterator((None, 1.0), (None, 2.0), (Some(2), 3.0), (Some(2), 4.0), (Some(3), 5.0)))
    }

    describe("should work with null values") {
      test(() => Iterator((1, "1.0"), (1, null), (2, null), (2, "4.0"), (3, "5.0")))
    }

    describe("should work with None values") {
      test(() => Iterator((1, Some(1.0)), (1, None), (2, None), (2, Some(4.0)), (3, Some(5.0))))
    }

    describe("should work with one 1-element groups") {
      test(() => Iterator((1, 1.0)))
    }

    describe("should work with many 1-element groups") {
      test(() => Iterator((1, 1.0), (2, 2.0), (3, 3.0)))
    }

    describe("should work with one group") {
      test(() => Iterator((1, 1.0), (1, 2.0), (1, 3.0)))
    }

    describe("should work with many groups") {
      test(() => Iterator((1, 1.0), (1, 2.0), (2, 3.0), (2, 4.0), (3, 5.0)))
    }

    def test[K: Ordering, V](func: () => Iterator[(K, V)]): Unit = {
      testWithUnconsumedGroups(func)
      testWithPartiallyConsumedGroups(func)
      testWithFullyConsumedGroups(func)
      testWithMultipleHasNext(func)
    }

    def testWithUnconsumedGroups[K: Ordering, V](func: () => Iterator[(K, V)]): Unit = {
      val existingKeys = func().map(_._1).toSet.toList

      // this does not consume any group iterators
      it("and unconsumed groups") {
        val git = new GroupedIterator(func())
        val actualKeys = git.map(_._1).toList
        assert(actualKeys === existingKeys)
      }

      // tests a specific group not being consumed at all
      def testUnconsumedKey(unconsumedKey: K, func: () => Iterator[(K, V)]): Unit = {
        // we expect all tuples (k, Some(v)), except for k == unconsumedKey, where we expect (k, None)
        // here we consume all groups (it.toList), which is tested elsewhere to work
        val expected = new GroupedIterator(func()).map {
          case (key, it) if key == unconsumedKey => it.toList; (key, Iterator(None))
          case (key, it) => (key, it.map(Some(_)))
        }.flatMap { case (k, it) => it.map(v => (k, v)) }.toList

        // here we do not consume the group with key `unconsumedKey`
        val actual = new GroupedIterator(func()).map {
          case (key, _) if key == unconsumedKey => (key, Iterator(None))
          case (key, it) => (key, it.map(Some(_)))
        }.flatMap { case (k, it) => it.map(v => (k, v)) }.toList

        assert(actual === expected)
      }

      // this does not consume the first group iterator
      it("and unconsumed first group") {
        if (existingKeys.nonEmpty) {
          val firstKey = existingKeys.last
          testUnconsumedKey(firstKey, func)
        }
      }

      // this does not consume the second group iterator
      it("and unconsumed second group") {
        if (existingKeys.length >= 2) {
          val secondKey = existingKeys.tail.head
          testUnconsumedKey(secondKey, func)
        }
      }

      // this does not consume the last group iterator
      it("and unconsumed last group") {
        if (existingKeys.nonEmpty) {
          val lastKey = existingKeys.last
          testUnconsumedKey(lastKey, func)
        }
      }
    }

    def testWithPartiallyConsumedGroups[K: Ordering, V](func: () => Iterator[(K, V)]): Unit = {
      val existingKeys = func().map(_._1).toSet.toList

      // this consumes only the first value of each group iterator
      it("and partially consumed groups") {
        val git = new GroupedIterator(func())
        val actualKeyValues = git.map { case (k, it) => (k, it.next()) }.toList
        val expectedKeyValues = func().toList.groupBy(_._1).mapValues(_.head).values.toMap
        val expectedKeyValuesOrdered = existingKeys zip existingKeys.map(expectedKeyValues)
        assert(actualKeyValues === expectedKeyValuesOrdered)
      }

      // tests a specific group not being consumed at all
      def testPartiallyConsumedKey(partiallyConsumedKey: K, func: () => Iterator[(K, V)]): Unit = {
        // we expect all tuples (k, v), except for k == unconsumedKey,
        // where we expect only the first tuple with k == partiallyConsumedKey
        // here we consume all groups (it.toList), which is tested elsewhere to work
        val expected = new GroupedIterator(func()).map {
          case (key, it) if key == partiallyConsumedKey => (key, Iterator(it.toList.head))
          case (key, it) => (key, it)
        }.flatMap { case (k, it) => it.map(v => (k, v)) }.toList

        // here we only consume the first element of the group with key `unconsumedKey`
        val actual = new GroupedIterator(func()).map {
          case (key, it) if key == partiallyConsumedKey => (key, Iterator(it.next()))
          case (key, it) => (key, it)
        }.flatMap { case (k, it) => it.map(v => (k, v)) }.toList

        assert(actual === expected)
      }

      // this consumes the first group iterator only partially
      it("and partially consumed first group") {
        if (existingKeys.nonEmpty) {
          val firstKey = existingKeys.last
          testPartiallyConsumedKey(firstKey, func)
        }
      }

      // this consumes the second group iterator only partially
      it("and partially consumed second group") {
        if (existingKeys.length >= 2) {
          val secondKey = existingKeys.tail.head
          testPartiallyConsumedKey(secondKey, func)
        }
      }

      // this consumes the last group iterator only partially
      it("and partially consumed last group") {
        if (existingKeys.nonEmpty) {
          val lastKey = existingKeys.last
          testPartiallyConsumedKey(lastKey, func)
        }
      }
    }

    def testWithFullyConsumedGroups[K: Ordering, V](func: () => Iterator[(K, V)]): Unit = {
      // this consumes all group iterators
      it("and fully consumed groups") {
        val expected = func().toList
        val actual = new GroupedIterator(func()).flatMap {
          case (k, it) => it.map(v => (k, v))
        }.toList
        assert(actual === expected)
      }
    }

    def testWithMultipleHasNext[K: Ordering, V](func: () => Iterator[(K, V)]): Unit = {
      it("and multiple calls to hasNext") {
        val iter = func()
        val isEmpty = iter.hasNext

        val git = new GroupedIterator(iter)
        assert(git.hasNext === isEmpty)
        assert(git.hasNext === isEmpty)
        assert(git.hasNext === isEmpty)

        while (git.hasNext) {
          assert(git.hasNext === true)
          assert(git.hasNext === true)
          git.next()
        }
      }
    }

  }

  describe("GroupIterator") {
    it("should not work with empty iterator") {
      assertThrows[NoSuchElementException] { new GroupIterator[Int, Double](Iterator.empty.buffered) }
    }

    describe("should iterate only over current key") {
      def test[K : Ordering, V](it: Seq[(K, V)], expectedValues: Seq[V]): Unit = {
        val git = new GroupIterator[K, V](it.iterator.buffered)
        assert(git.toList === expectedValues)
      }

      it("for null key") {
        test(Seq((null, 1.0), (null, 2.0), ("1", 3.0)), Seq(1.0, 2.0))
      }

      describe("for single key") {
        it("and single value") {
          test(Seq((1, 1.0)), Seq(1.0))
        }
        it("and multiple values") {
          test(Seq((1, 1.0), (1, 2.0)), Seq(1.0, 2.0))
          test(Seq((1, 1.0), (1, 2.0), (1, 3.0)), Seq(1.0, 2.0, 3.0))
        }
      }

      describe("for multiple keys") {
        it("and single value") {
          test(Seq((1, 1.0), (2, 2.0)), Seq(1.0))
        }
        it("and multiple values") {
          test(Seq((1, 1.0), (1, 2.0), (2, 3.0)), Seq(1.0, 2.0))
          test(Seq((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0)), Seq(1.0, 2.0, 3.0))
        }
      }
    }
  }

}
