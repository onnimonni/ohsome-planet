class OhsomePlanet < Formula
  desc "Tool to transform OSM (history) PBF files into GeoParquet"
  homepage "https://github.com/GIScience/ohsome-planet"
  url "https://github.com/GIScience/ohsome-planet/archive/refs/tags/1.1.0.tar.gz"
  sha256 "d77dcc90e3a7534423a4286af03a1463a3738dbd6d35afb29eed73077abeb452"
  license "GPL-3.0-or-later"

  head "https://github.com/GIScience/ohsome-planet.git", branch: "main"

  # Define dependencies required for building and running the tool.
  # 'maven' is needed at build time to compile the Java source.
  # 'openjdk' is required at runtime to execute the compiled JAR file.
  depends_on "maven" => :build
  depends_on "openjdk"

  def install
    # The 'pom.xml' in the project defines how to build the package.
    # We use Maven to build the project from source.
    # "mvn clean package" compiles the code and packages it into a JAR file.
    # "-DskipTests" is used to speed up the build process by skipping the test suite.
    system "mvn", "clean", "package", "-DskipTests"

    # The result of the build is an "uber-jar" (a JAR file containing all dependencies).
    # We install this JAR into the 'libexec' directory, which is the standard Homebrew
    # location for application data and libraries.
    libexec.install "ohsome-planet-cli/target/ohsome-planet.jar"

    # To make the tool easily executable from the command line, we create a
    # wrapper script in the 'bin' directory.
    # This script will execute the Java application.
    (bin/"ohsome-planet").write <<~EOS
      #!/bin/bash
      # This script executes the ohsome-planet JAR file using the Java runtime
      # provided by the openjdk dependency.
      # It passes all command-line arguments ("$@") to the Java application.
      exec "#{Formula["openjdk"].opt_bin}/java" -jar "#{libexec}/ohsome-planet.jar" "$@"
    EOS
  end

  test do
    # The test block verifies that the installation was successful.
    # We run the installed command with the '--help' flag.
    # The 'system' command will fail the test if the command returns a non-zero exit code.
    # A successful execution of the help command indicates the tool is installed correctly.
    assert_match "Usage: ohsome-planet", shell_output("#{bin}/ohsome-planet --help")
  end
end
