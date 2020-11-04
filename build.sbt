name := "AUDESOME"
version := "0.1"
scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5"
libraryDependencies += "org.apache.commons" % "commons-collections4" % "4.4"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"
libraryDependencies += "com.spatial4j" % "spatial4j" % "0.5"
libraryDependencies += "com.google.guava" % "guava" % "29.0-jre"
libraryDependencies += "de.micromata.jak" % "JavaAPIforKml" % "2.2.1"
libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default"
libraryDependencies += "com.github.workingDog" %% "scalakml" % "1.3"
libraryDependencies += "de.micromata.jak" % "JavaAPIforKml" % "2.2.1"
libraryDependencies += "org.rogach" %% "scallop" % "3.5.1"

// set the main class for 'sbt run'
mainClass in (Compile, run) := Some("workflow.Main")