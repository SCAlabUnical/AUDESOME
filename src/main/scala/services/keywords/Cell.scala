package services.keywords

class Cell(var lat: Double=0.0, var lon: Double=0.0){

  override def hashCode(): Int =
    31 * (
      lat.##
      ) + lon.##

  override def equals(o: Any): Boolean = {
    if (o == null) return false
    if (getClass ne o.getClass) return false
    val other = o.asInstanceOf[Cell]
    if (this.lat != other.lat)
      return false
    if (this.lon != other.lon)
      return false
    true
  }

  override def toString: String = super.toString

  def main(args: Array[String]): Unit = {

  }

}