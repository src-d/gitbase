name := "enry-java"
organization := "tech.sourced"
version := "1.6.6"

sonatypeProfileName := "tech.sourced"

// pom settings for sonatype
homepage := Some(url("https://github.com/src-d/enry"))
scmInfo := Some(ScmInfo(url("https://github.com/src-d/enry"),
                            "git@github.com:src-d/enry.git"))
developers += Developer("abeaumont",
                        "Alfredo Beaumont",
                        "alfredo@sourced.tech",
                        url("https://github.com/abeaumont"))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
pomIncludeRepository := (_ => false)

crossPaths := false
autoScalaLibrary := false
publishMavenStyle := true
exportJars := true

val SONATYPE_USERNAME = scala.util.Properties.envOrElse("SONATYPE_USERNAME", "NOT_SET")
val SONATYPE_PASSWORD = scala.util.Properties.envOrElse("SONATYPE_PASSWORD", "NOT_SET")
credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  SONATYPE_USERNAME,
  SONATYPE_PASSWORD)

val SONATYPE_PASSPHRASE = scala.util.Properties.envOrElse("SONATYPE_PASSPHRASE", "not set")

useGpg := false
pgpSecretRing := baseDirectory.value / "project" / ".gnupg" / "secring.gpg"
pgpPublicRing := baseDirectory.value / "project" / ".gnupg" / "pubring.gpg"
pgpPassphrase := Some(SONATYPE_PASSPHRASE.toArray)

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test

unmanagedBase := baseDirectory.value / "lib"
unmanagedClasspath in Test += baseDirectory.value / "shared"
unmanagedClasspath in Runtime += baseDirectory.value / "shared"
unmanagedClasspath in Compile += baseDirectory.value / "shared"
testOptions += Tests.Argument(TestFrameworks.JUnit)

publishArtifact in (Compile, packageBin) := false

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = None)
}

addArtifact(artifact in (Compile, assembly), assembly)

isSnapshot := version.value endsWith "SNAPSHOT"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
