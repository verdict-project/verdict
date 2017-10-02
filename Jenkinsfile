// Jenkins server hosted on: http://elgordo.eecs.umich.edu:8080/

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'mvn clean'
                sh 'mvn compile -Predshift41'
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
                sh 'mvn package -Papache-spark-scala2.10'
                sh 'mvn package -Papache-spark2-scala2.10'
                sh 'mvn package -Papache-spark-scala2.11'
                sh 'mvn package -Papache-spark2-scala2.11'
                sh 'mvn package -Pcdh5.11.1'
                sh 'mvn package -Pcdh5.12.1'
                sh 'mvn package -Pmapr-spark2.0.1'
                sh 'mvn package -Pmapr-spark2.1.0'
                sh 'mvn package -Predshift41'
                sh 'python release/release_jars.py'
                sh 'mvn clean'
            }
        }
    }
}
