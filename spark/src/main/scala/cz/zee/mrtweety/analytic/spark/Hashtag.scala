package cz.zee.mrtweety.analytic.spark

/**
  * A single hashtag.
  *
  * @author Jakub Horak
  */
@SerialVersionUID(100L)
class Hashtag(val text: String) extends Serializable {

  override def hashCode(): Int = {
    text.toLowerCase.hashCode
  }

  override def equals(obj: scala.Any): Boolean = {
    hashCode().equals(obj.hashCode())
  }
}
