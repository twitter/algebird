import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.{ Incrementor, AsyncSummer }
import com.twitter.util.Future

case class InstrumentedNullSummer[Key, Value](tuplesIn: Incrementor, tuplesOut: Incrementor)(implicit semigroup: Semigroup[Value])
  extends AsyncSummer[(Key, Value), Map[Key, Value]] {
  def flush: Future[Map[Key, Value]] = Future.value(Map.empty)
  def tick: Future[Map[Key, Value]] = Future.value(Map.empty)
  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {

    val r = Semigroup.sumOption(vals.map { inV =>
      tuplesIn.incr
      Map(inV)
    }).getOrElse(Map.empty)
    tuplesOut.incrBy(r.size)
    Future.value(r)
  }
  override val isFlushed: Boolean = true
}