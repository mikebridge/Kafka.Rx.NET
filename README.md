[![Build status](https://ci.appveyor.com/api/projects/status/2qudhjt1jmrb0je0?svg=true)](https://ci.appveyor.com/project/mikebridge/kafka-rx-net)

# C# to Kafka via Rx

This project demonstrates how to listen to a continuous stream of Kafka events from C#/.Net, using IObservables.

## Requirements:

1) All four parts of the the [Confluent Platform](http://docs.confluent.io/1.0.1/) need to be up and running: Zookeeper, Kafka, the Schema Registry and the Rest Client.  I'm running them on a Linux virtual machine, and I didn't need to do any configuration changes.

In four different consoles, run these commands:
```bash
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

$ ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties

$ ./bin/kafka-server-start ./etc/kafka/server.properties

$ ./bin/kafka-rest-start
```
2) The examples here require the [Confluent Rest Client](https://github.com/josephjeganathan/Confluent.RestClient), which is installed via NuGet.

## Use Confluent.RestClient to create a Client & Consumer

To connect via the Confluent.RestClient , you have to create an [`IConfluentClient`](https://github.com/josephjeganathan/Confluent.RestClient/blob/master/src/Confluent.RestClient/IConfluentClient.cs), then use that client to request a [`ConsumerInstance`](https://github.com/josephjeganathan/Confluent.RestClient/blob/master/src/Confluent.RestClient/Model/ConsumerInstance.cs).  (The confluent [REST API documentation](http://confluent.io/docs/current/kafka-rest/docs/index.html) has some more information on creating a client and consumer.)  An example of creating a client:

```C#
public class KafkaClientSettings : IConfluentClientSettings
{
    public string KafkaBaseUrl { get; set; }
}

public static IConfluentClient CreateConfluentClient(String baseUrl)
{
    var settings = new KafkaClientSettings
    {
        KafkaBaseUrl = baseUrl
    };
    return new ConfluentClient(settings);
}

///...
var client = CreateConfluentClient("http://192.168.1.2:8082");

```

The client can then be used to create a ConsumerInstance via a CreateConsumerRequest.  The creation request can specify a Consumer Group, and an Instance Id within that group.  Each Consumer group will receive a copy of every message, but only one Instance within each group will process that message [Ref](http://kafka.apache.org/documentation.html#intro_consumers).

A consumer instance can be created like this:

```C#
public static async Task<ConfluentResponse<ConsumerInstance>> CreateConsumerInstance(
    IConfluentClient client,
    string instanceId,
    string consumerGroupName)
{
    var request = new CreateConsumerRequest
    {
        AutoCommitEnabled = true,
        InstanceId = instanceId,
        MessageFormat = MessageFormat.Avro
    };

    return await client.CreateConsumerAsync(consumerGroupName, request);
}

public static ConsumerInstance ConsumerInstance(
    IConfluentClient client,
    string instanceId,
    string consumerGroupName)
{
    var consumerInstanceTask = Setup.CreateConsumerInstance(client, instanceId, consumerGroupName).Result;

    if (!consumerInstanceTask.IsSuccess())
    {
        throw new ApplicationException("Error " + consumerInstanceTask.Error.ErrorCode + ": " + consumerInstanceTask.Error.Message);
    }
    return consumerInstanceTask.Payload;
}
```

# Create an RX Observable

Once you have a client and a consumerInstance, you can subscribe to a topic.  The observable operates as a non-blocking infinte loop, waking up the thread every so often to poll the API.  You can specify the `interval` as a TimeSpan.

In this case, we don't want the IObservable to terminate if an error occurs, so the RxConsumer will return a [Try](https://github.com/mikebridge/Kafka.Rx.NET/blob/master/Kafka.Rx.NET/Try.cs) as the payload.  This means you will either receive an Exception wrapped in a Failure (which subclasses Try) or the expected result wrapped in a Success (which also subclasses Try).  

```
    var consumer = new RxConsumer(client, consumerInstance, topic);
 
    // Act
    var observable = consumer.GetRecordStream<String, RxConsumerTests.LogMessage>(
        TimeSpan.FromSeconds(5),
        ThreadPoolScheduler.Instance)
            .Subscribe(successResult =>
            {
                 Console.WriteLine("Success: " + successResult.IsSuccess);
                 if (successResult.IsSuccess)
                 {
                     Console.WriteLine(successResult.Value.Key + "=" + successResult.Value.Value.Message);
                 }
                 else
                 {
                     Console.WriteLine("ERROR: " + successResult.Exception.Message);
                 }
            }
    );

```

When you're done with the observable subscription, make sure you call `Dispose()` as you normally would.  This will terminate the polling thread.

If you are shutting down the client, you need to deregister it with the server:

```c#
    client.DeleteConsumerAsync(consumerInstance)
```

## Console stream listener

The console listener is `Kafka.Rx.NET.Console.exe`.  You can run it with no arguments to see the a list of command-line arguments.  (The demo console listener is listening specifically for a LogMessage Avro message.)

This example will create a consumer with instanceid "0" in the group "mytestgroup", listening to "mytopic".  By default it polls, then waits 500ms before repeating.

```cmd
> Kafka.Rx.NET.Console.exe -u http://myhost:8082 -i 0 -g mytestgroup -t mytopic
```

## Add an Event to the Stream

This will add two "LogMessage" events to the "rxtest" topic.  The two messages should appear in the console listener.

```bash
curl -i -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" --data '{ "value_schema": "{\"type\": \"record\", \"name\": \"LogMessage\", \"fields\": [{\"name\": \"message\", \"type\": \"string\"}]}", "records": [ {"value": {"message": "Hello #1"}},{"value": {"message": "Hello #2"}}]}' http://localhost:8082/topics/rxtest
```

## Unit Testing

The observable can be unit tested using the `TestScheduler` from the  [`Microsoft.Reactive.Testing`](https://www.nuget.org/packages/Rx-Testing/) library.  In production we would schedule the API calls using `ThreadPoolScheduler.Instance`, but we can simulate clock ticks using the `TestScheduler`.

[RxConsumerTests.cs](https://github.com/mikebridge/Kafka.Rx.NET/blob/master/Kafka.Rx.NET.Tests/RxConsumerTests.cs) makes use of `TestScheduler.AdvanceBy(...)` to simulate the passage of time, and to keep our unit tests quick and deterministic.

## TODO

- Add different commit strategies
- Produce events via observable
- Handle broker leader changes / find leader
