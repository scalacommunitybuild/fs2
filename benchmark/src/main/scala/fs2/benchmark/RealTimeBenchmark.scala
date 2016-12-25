package fs2.benchmark

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import fs2.{Scope => _, _}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.duration._



@State(Scope.Thread)
class RealTimeBenchmark extends BenchmarkUtils {

  implicit val SCh = Scheduler.fromFixedDaemonPool(cores)
  implicit val S = scaledStrategy

  val samples: Int = 100
  val sampleDelay: FiniteDuration = 20.millis

  @GenerateN(1, 4, 100, 250, 500, 1000, 2000, 5000, 10000, 20000)
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def baseline(N: Int): Int = {
    def sampleStream(thunk: => Unit) = {
      val remains = new AtomicInteger(samples)
      def goNext : Unit= {
        SCh.scheduleOnce(sampleDelay)({

          if (remains.decrementAndGet() > 0) { Blackhole.consumeCPU(500); goNext }
          else thunk
        })
        ()
      }
      goNext
    }

    val counter = new CountDownLatch(N)

    (1 to N).foreach { _ =>
      sampleStream { counter.countDown() }
    }

    counter.await()
    N*samples
  }

  @GenerateN(1, 4, 100, 250, 500, 1000, 2000, 5000, 10000)
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def taskBased(N:Int):Int = {
    def singleStream:Task[Unit] = {
      def go(rem:Int):Task[Unit] = {
        if (rem > 0) {
          Task.schedule(Blackhole.consumeCPU(500),sampleDelay).flatMap { _ => go(rem-1) }
        } else Task.now(())
      }

      Task.start(go(samples)).flatMap(identity)
    }

    val counter = new CountDownLatch(N)

    (1 to N).foreach { _ =>
      singleStream.unsafeRunAsync { _ =>
        counter.countDown()
      }
    }
    counter.await()
    N*samples
  }

  @GenerateN(1, 4, 100, 250, 500, 1000, 2000, 5000)
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def streamAsync(N:Int):Int = {
    def stream = {
      Task.start(Stream.range[Task](0,samples).flatMap { _ =>
        time.sleep[Task](sampleDelay) ++ Stream.eval(Task.delay(Blackhole.consumeCPU(500)))
      }.run).flatMap(identity)
    }

    val counter = new CountDownLatch(N)

    (1 to N).foreach { _ =>
      stream.unsafeRunAsync { _ =>
        counter.countDown()
      }
    }
    counter.await()
    N*samples
  }

  @GenerateN(1, 4, 100, 250, 500, 1000, 2000)
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def streamJoin(N:Int):Int = {
    def stream:Stream[Task,Unit] = {
      Stream.range[Task](0,samples).flatMap { _ =>
        time.sleep[Task](sampleDelay) ++ Stream.eval(Task.delay(Blackhole.consumeCPU(500)))
      }
    }

    val counter = new CountDownLatch(1)


    concurrent.join(Int.MaxValue){
      Stream.range[fs2.Task](0,N).map { _ =>
        stream.drain
      }
    }.run.unsafeRunAsync { _ =>
      counter.countDown()
    }

    counter.await()
    N*samples
  }

}

