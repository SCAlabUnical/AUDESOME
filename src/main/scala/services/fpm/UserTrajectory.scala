package services.fpm

class UserTrajectory (var username: String = "", var daytime: Long = 0L){

  override def hashCode(): Int =
    31 * (
      username.##
      ) + daytime.##

  override def equals(o: Any): Boolean = {
    if (o == null) return false
    if (getClass ne o.getClass) return false
    val other = o.asInstanceOf[UserTrajectory]
    if(username.equals(other.username) && daytime.equals(other.daytime))
      return true
    false
  }

  override def toString: String = {
    "Trajectory %s, day %s".format(username, daytime.toString)
  }
}
