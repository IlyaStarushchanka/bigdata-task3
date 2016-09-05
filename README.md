# bigdata-task3 "MapReduce"

Build project 
```
mvn clean install
```
Run tags count task

```
yarn jar ${PathToProject}/bigdata-task3/target/bigdata-task3-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.mapreduce.TagsCount <in> <out> <inoptional>
```

Where

`in` - path to input file,

`out` - path for output,

`inoptional` - optional parameter, path to file with stopwords (1 word in 1 line).
