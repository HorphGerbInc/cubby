pipeline {
  agent {label 'Linux&&Leiningen'}
  stages {
    stage('Test') {
      steps {
        checkout scm
        sh 'lein test'
      }
    }
  }
}
