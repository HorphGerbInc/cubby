pipeline {
  agent {label 'Linux&&Leiningen'}
  stages {
    stage('Test') {
      steps {
        deleteDir()
        checkout scm
        sh 'lein test'
      }
    }
  }
}
