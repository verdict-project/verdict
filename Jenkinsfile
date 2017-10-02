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
                sh 'mvn test -Pcdh5.11.1 -DskipTests=false'
            }
        }
        stage('Deploy') {
            steps {
                sh 'python release/update_build_number.py'
                sh 'mvn package -Pcdh5.11.1'
                sh 'mvn package -Predshift'
                sh 'mvn package -Papache'
                sh 'python release/release_jars.py'
                sh 'mvn clean'
            }
        }
    }
}
