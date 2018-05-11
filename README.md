# RedisBatchesBenchmark
JMH benchmark for Redis command batching

SO question: https://stackoverflow.com/questions/48359548/how-to-call-incrby-and-expire-most-efficiently-in-redis

Usage:  
mvn clean install  
java -jar target/RedisBatchesBenchmark-1.0-SNAPSHOT.jar -i 5 -wi 2 -f 5 -t 4 -r 5 -w 10 -o ~/RedisBatches.log

Here -t is the number of threads to test on.
