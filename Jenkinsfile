pipeline {
    agent {
        docker {
            image "eclipse-temurin:21.0.6_7-jdk-alpine-3.21"
            args '-v $HOME/.gradle:/var/lib/jenkins/.gradle:rw -e GRADLE_USER_HOME=/var/lib/jenkins/.gradle'
        }
    }
    options {
        disableConcurrentBuilds abortPrevious: true
        buildDiscarder(logRotator(numToKeepStr: '2', artifactNumToKeepStr: '2'))
    }
    stages {
        stage('Unit & Integration Tests') {
            steps {
                script {
                    try {
                        sh './gradlew clean test --no-daemon'
                    } finally {
                        junit '**/build/test-results/test/*.xml'
                    }
                }
            }
        }
        stage('Long run test') {
            steps {
                script {
                    try {
                        sh './gradlew --no-daemon --stacktrace build -x test longRunTest'
                    } finally {
                        junit '**/build/test-results/test/*.xml'
                    }
                }
                archiveArtifacts artifacts: 'longRunTest.out.txt', fingerprint: true
                archiveArtifacts artifacts: 'longRunTest.jfr', fingerprint: true
            }
        }
    }
    post {
        // Clean after build
        always {
            cleanWs()
        }
    }
}