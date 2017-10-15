pipeline {
  agent {label 'Linux&&Leiningen'}
  stages {
    stage('Test') {
      checkout scm
      sh 'lein test'
    }
  }
}
