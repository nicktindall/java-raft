pipeline {
    agent any
    options {
        disableConcurrentBuilds abortPrevious: true
        buildDiscarder(logRotator(numToKeepStr: '2', artifactNumToKeepStr: '2'))
    }
    stages {
        stage('Setup parameters') {
            steps {
                script {
                    properties([
                        parameters([
                            choice(
                                choices: ['Netty', 'Simulated'],
                                name: 'NETWORK_TYPE'
                            )
                        ])
                    ])
                }
            }
        }
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
        stage('Long run Simulated') {
            when {
                expression {
                    return params.NETWORK_TYPE == 'Simulated'
                }
            }
            steps {
                script {
                    try {
                        sh './gradlew --no-daemon --stacktrace build -x test longRunSimulated'
                    } finally {
                        junit '**/build/test-results/test/*.xml'
                    }
                }
                archiveArtifacts artifacts: 'longRunTest.out.txt', fingerprint: true
                archiveArtifacts artifacts: 'longRunTest.jfr', fingerprint: true
            }
        }
        stage('Long run Netty') {
            when {
                expression {
                    return params.NETWORK_TYPE == 'Netty'
                }
            }
            steps {
                script {
                    try {
                        sh './gradlew --no-daemon --stacktrace build -x test longRunNetty'
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