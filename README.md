# log-java-producer

基础日志生产者模块，提供统一的日志生产接口，支持多种日志框架的适配。

## 功能

- 提供日志生产的核心接口
- 支持日志事件的统一封装
- 适配多种日志框架（Log4j、Log4j2、Logback）

## 使用方式

1. 引入依赖（Maven）：
```xml
<dependency>
  <groupId>io.github.log-java-producer</groupId>
  <artifactId>log-java-producer</artifactId>
  <version>0.1.0</version>
</dependency>
```

2. 在代码中调用日志生产接口：
```java
LogProducer producer = new LogProducer();
producer.log("INFO", "This is a log message.");
```

## 许可证

Apache-2.0