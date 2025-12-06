package hello
import hello.Exercise

/*
  ---

  ### 3. Types, generics, and “implicits just enough”

  Goal: understand library signatures and “why does this implicit not resolve”.

  Focus topics (keep it pragmatic):

 * Type parameters: `def f[A](xs: List[A]): Int = xs.size`.
 * Traits with type parameters (`trait LayerReader[K, V]`).
 * Implicits as:

 * “Values found automatically”: `implicit val ec = ExecutionContext.global`.
 * “Typeclass instances”: `implicit val ordering: Ordering[TileId] = ...`
 * Context bounds syntax: `def f[A: Ordering](xs: List[A])`.

  Exercise:

 * Define a simple typeclass:

    ```scala
    trait Show[A] { def show(a: A): String }
    object Show {
      implicit val intShow: Show[Int] = a => s"Int($a)"
    }
    def printShow[A: Show](a: A): Unit =
      println(implicitly[Show[A]].show(a))
    ```

    and call it with an `Int`. Just to demystify the pattern you will see in libraries.
 */

object Ex03 extends Exercise {
  override def run(): Unit = {

    trait Show[A] { def show(a: A): String }
    object Show {
      implicit val intShow: Show[Int] = a => s"Int($a)"
    }
    def printShow[A: Show](a: A): Unit =
      println(implicitly[Show[A]].show(a))
  }
}
