#
#  En este archivo escriba un script en bash para ejecutar
#  el job en Hadoop usando el modo pseudo-distribuido y 
#  recuperar el resultado de ejecutar el job
#
#  >>>> Escriba su codigo a partir de esta línea <<<<
#
hadoop fs -mkdir homework
hadoop fs -mkdir homework/input/
hadoop fs -copyFromLocal *.csv homework/input/
#hadoop fs -copyFromLocal mapper.py 
#hadoop fs -copyFromLocal reducer.py 
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  homework/input/ \
    -output homework/output/ \
    -mapper "python3 $PWD/mapper.py" \
    -reducer "python3 $PWD/reducer.py"
hadoop fs -cat homework/output/*
