// Jenkins server hosted on: http://elgordo.eecs.umich.edu:8080/

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'mvn clean'
                sh 'mvn compile -Predshift'
            }
        }
        stage('Test') {
            steps {
                sh 'mvn test -Pimpala -DskipTests=false'
                sh 'mvn clean'
            }
        }
        stage('Deploy') {
            steps {
                sh 'python release/update_build_number.py'
                sh 'mvn package'
                sh 'python release/release_jars.py'
                sh 'mvn clean'
            }
        }
    }
}
