package services.fpm
import java.time.Instant
import java.time.ZoneId

class SingleTrajectory(var poi: String = "", var daytime: Long = 0L) {
  override def hashCode(): Int =
    31 * (
      poi.##
      ) + daytime.##

  override def equals(o: Any): Boolean = {
    if (o == null) return false
    if (getClass ne o.getClass) return false
    val other = o.asInstanceOf[SingleTrajectory]
    if(poi.equals(other.poi) && daytime.equals(other.daytime))
      return true
    false
  }

  override def toString: String = {
    "Single Trajectory for poi %s, timestamp %s".format(poi, Instant.ofEpochMilli(daytime).atZone(ZoneId.systemDefault).toLocalDate.toString)
  }
}
