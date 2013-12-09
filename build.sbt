name := "masterworker"

organization := "de.uniwue.info2"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

javacOptions in (Compile, compile) ++= Seq("-source", "1.7", "-target", "1.7")

resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"

resolvers += {
  Resolver.sftp("internal", "wubi007", 22, "www/web/repos")(Patterns("/[organisation]/[module]/ivys/ivy-[revision].xml" :: Nil,"/[organisation]/[module]/[type]s/[organisation]-[artifact]-[revision].[ext]" :: Nil, false)) as("maven", (Path.userHome / "ssh" / "gradle_id_rsa").asFile)
}

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.2.3"

publishTo := {
  Some(Resolver.ssh("internal", "wubi007", 22, "www/web/repos")(Patterns("/[organisation]/[module]/ivys/ivy-[revision].xml" :: Nil, "/[organisation]/[module]/[type]s/[organisation]-[artifact]-[revision].[ext]":: Nil, false)) as("maven", (Path.userHome / "ssh" / "gradle_id_rsa").asFile) withPermissions("0666"))
}
