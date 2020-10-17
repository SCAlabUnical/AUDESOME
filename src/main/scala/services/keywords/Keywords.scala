package services.keywords

class Keywords(var term: String="", var syn: Set[String] = Set(), var score: Double = 0.0, var cell: Cell = null){

  override def hashCode(): Int = {
    term.hashCode
  }

  override def equals(o: Any): Boolean = {
    if (o == null) return false
    if (getClass ne o.getClass) return false
    val other = o.asInstanceOf[Keywords]
    term.equals(other.term)
  }

  override def toString: String = {
    return "Keywords : %s with score %f".format(term,score)
  }

}