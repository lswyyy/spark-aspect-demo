# spark-aspect-demo
使用 aspectj 对 spark 方法进行一些增强，例如 demo 中对 `spark.sql()` 方法注入对表权限控制

## Usage
### IDEA
set configuration

`VM Options  -javaagent:/path/to/aspectjweaver-1.9.1.jar`


### spark submit
```
spark.driver.extraClassPath /path/to/spark-aspectj-demo.jar
spark.driver.extraJavaOptions -javaagent:/path/to/aspectjweaver-1.9.1.jar
```

https://blog.csdn.net/lsshlsw/article/details/82357199
