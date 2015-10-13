# C# to Kafka via Rx

This project shows how to listen to a stream of events from Kafka to C#, using IObservables.

## Requirements:

1) All four parts of the the [Confluent Platform](http://docs.confluent.io/1.0.1/) need to be up and running: Zookeeper, Kafka, the Schema Registry and the Rest Client.  I'm running them on a Linux virtual machine, and I didn't need to do any configuration.

2) The examples here require the [Confluent Rest Client](https://github.com/josephjeganathan/Confluent.RestClient), which is installed via NuGet.

## Connection

To connect via the Confluent.RestClient , you have to create an `IConfluentClient`, then use the client to request a ConsumerInstance.  


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
```

Then you use that client to create a ConsumerInstance via a CreateConsumerRequest.  You can specify a Consumer Group, and an Instance Id within that group.  (Each Consumer group will receive a copy of every message, but only one Instance within each group will process that message.)

Your setup might look like this:

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

# Example

Once you have a client and a consumerInstance, you can subscribe to a topic.

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

When you're done with the observable subscription, call `Dispose()` as you normally would.  This will cancel the polling.

If you are shutting down the client, you should also deregister it with the server:

```c#
    client.DeleteConsumerAsync(consumerInstance)
```

##  DEMO
```bash
curl -i -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" --data '{ "value_schema": "{\"type\": \"record\", \"name\": \"LogMessage\", \"fields\": [{\"name\": \"message\", \"type\": \"string\"}]}", 
"records": [ {"value": {"message": "Hello From Linux #1"}},{"value": {"message": "Hello From Linux #2"}}]}' http://localhost:8082/topics/rxtest
```

