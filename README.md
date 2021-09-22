# Introduction

Simple spring boot application with a singleton KafkaProducer which can be instantiated with properties from src/main/resources/application.yml

# Build and run

Update application.yml with the right bootstrap server setting.

```
> mvn spring-boot:run
```

# Test

```
> curl -X POST -H "Content-Type: application/json" http://localhost:8080/publish -d '{"name": "world" }'
```


