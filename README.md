# EquiTruss_Spark

THE PROgram takes 4 parameters.
1) Number of partitions
2)  Maximum number of iterations
3)  Output directory
4) input file

The Community Search Program takes following parameters.
1) Number of partitions
2)  Maximum number of iterations
3)  Output directory
4) SuperEdges input
5) k (minimum trussness)
6) List of Vertices with their supernodes
7) List of superNodes with the constituent vertices.


The command used to submit application to an active spark master node is 

spark-submit --class <className>  --master <master address> <jar file> command line arguments.
  
  example : \bin\spark-submit  --class wordscount  --master spark://karthik-Inspiron-5379:7077   --deploy-mode cluster  /home/karthik/IdeaProjects/wordcount/target/scala-2.11/wordcount_2.11-0.1.jar 1 1 /home/karthik/IdeaProjects/wordcount/output_az /home/karthik/IdeaProjects/wordcount/email.txt
