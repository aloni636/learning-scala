package hello

trait Exercise {
  final def name: String = {
    // NOTE: We .stripSuffix("$") because singletons in scala are represented in JVM as "<classname>$"
    //       in order to differentiate them from classes sharing the same name and forming companionship
    this.getClass().getSimpleName().stripSuffix("$")
  }
  def run(): Unit
}
