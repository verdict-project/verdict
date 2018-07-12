FROM maven:3-jdk-8
WORKDIR /verdictdb
ADD . /verdictdb
CMD ["mvn", "test"]