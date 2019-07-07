# Commodity Spark Streaming

## Prepare

* Java 8
* Maven 3

## File

### com.spark.dom

* Order.java

  `Order` 用于Kafka序列号和反序列化，使用 `alibaba fastjson` 转化成`String`写给Kafka

* Item.java

  `Item` ，`Order`的一部分

### com.spark.commodity

* OrderProcessApp.java

  __`main`__ 函数所在

  * 负责连接Kafka，subscribe topics, 获取Stream。
  * 获取MySQL连接配置，静态配置或者通过命令行参数获取（`Connection` 在每个` Checker` 里创建）
  * 获取Zookeeper服务器配置，硬编码或者从命令行参数获取
  * Spark Streaming逻辑，调用Spark API处理`DStream`

  __提示__

  `main(...)` 函数中， `DStream.map()`这些`map()`不会立刻执行，在`ssc.start()`后才开始获取流并处理。`ssc.awaitTermination()`会等待结束。

* Checker.java

  （收银员）负责处理一条订单~~，实现了Zookeeper的`Watcher`接口，即`process(...)~~`。

  * 连接Zookeeper
    * 在Zookeeper上锁定某些商品(比如`create /commodity/<commodity id>`获取对应id商品的锁)
    * 从Zookeeper获取汇率
    * 更新Zookeeper上对应币种交易总额
    * 将`result id`写回Zookeeper中对应`Order`处（Spring负责创建znode并监视）
  * 连接MySQL
    * 创建事务（`Connection.setAutoCommit(fasle)`开启事务）
    * 获取商品数据
    * 更新商品数据
    * 写入`result`





