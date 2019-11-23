Assignment 3 - part 2

How to run:
1. Build and package the project in Intellij to get a jar file named assn3-part2_2.11-0.1.jar under the path /target/scala-2.11/.

2. Upload the jar file and the dataset to AWS S3.

3. Run it on a cluster with the following parameters.
spark-submit --deploy-mode cluster --class GraphX <jar Path> <input file> <output file>
