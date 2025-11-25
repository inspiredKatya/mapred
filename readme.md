Примеры MapReduce на Java.

Main-Class в build.gradle пока надо переписывать для запуска разных задач. 
src/resources - файлы, которые надо поместить в hdfs для запуска примеров.

Можно указать как Main-Class:
- WordCount - подсчет количества слов. Можно перед тем, как его запускать, создать большой файл со словами, используя генератор слов из примеров MapReduce в поставке hadoop
```shell
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar randomtextwriter words-out
hadoop jar  map-reduce-1.0.jar words-out wordscount-out
 ```
- ProductClickStarter - анализ кликов по продуктам, используя replicated join (соединение на стороне отображения с использованием распределенного кеша)
Большие данные - clicks, products помещаются в памяти (кеше)
Рассчитывается количество просмотров, покупок по категориям, конверсия. 
```shell
hdfs dfs -mkdir replicated
hdfs dfs -put ...replicated/clicks-big.csv replicated/clicks
hdfs dfs -put ...replicated/products.csv replicated/products
hadoop jar map-reduce-1.0.jar replicated/products replicated/clicks replicated-out
```

- CompositeJoinStarter - объединяет продукты и заказы и продукты и пользователей (composite join), используя repartition join (соединение на стороне свертки)
Большой файл orders, остальные medium-size.
```shell
hdfs dfs -mkdir repartition
hdfs dfs -put ...repartition/customers.csv repartition/customers
hdfs dfs -put ...repartition/products.csv repartition/products
hdfs dfs -put ...repartition/orders-big.csv repartition/orders
hadoop jar map-reduce-1.0.jar repartition/customers repartition/orders repartition/products repartition-out
```

- SemiJoinStarter - решает задачу найти активных пользователей (тех, которые оформляли заказ в течение последних 30 дней)
Используется semi-join (полусоединение на стороне свертки с фильтрацией на стороне отображения)
  Большой файл customers, orders уже отфильтрован по дате (заказы за последние 30 дней).
```shell
hdfs dfs -mkdir semijoin
hdfs dfs -put ...semijoin/customers-big.csv semijoin/customers
hdfs dfs -put ...semijoin/recent_orders.csv semijoin/orders
hadoop jar map-reduce-1.0.jar semijoin/orders semijoin/customers semijoin-tmp semijoin-out
```

