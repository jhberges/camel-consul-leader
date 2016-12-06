node {
	def mvnHome = tool 'M3'

	stage('Compile') {
		checkout scm;
  		sh "${mvnHome}/bin/mvn -B compile"
	}
	stage('Assemble') {
  		sh "${mvnHome}/bin/mvn -B package"
	}
	stage('Test') {
  		sh "${mvnHome}/bin/mvn -B test"
	}
}

