pipeline {
    agent any
    options {
        disableConcurrentBuilds abortPrevious: true
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
}