using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using Confluent.RestClient;
using Confluent.RestClient.Model;
using NUnit.Framework;

namespace Kafka.Rx.NET.Tests
{
    [TestFixture]
    public class RxConsumerIntegrationTest
    {
        /// <summary>
        /// At the moment this just listens to the stream
        /// </summary>
        [Test]
        [Category("Integration")]
        public void KafkaObservable_Should_Provide_An_Observable_Stream()
        {
            // Arrange
            String instanceId = "0";
            string consumerGroupName = "rxtestgroup";
            string topic = "rxtest";
            ConsumerInstance consumerInstance = null;

            using (var client = Setup.CreateConfluentClient("http://192.168.79.137:8082"))
            {
                try
                {
                    // in production this should be written without blocking.
                    consumerInstance = Setup.ConsumerInstance(client, instanceId, consumerGroupName);
                    var consumer = new RxConsumer(client, consumerInstance, topic);

                    // Act
                    var observable = consumer.GetRecordStream<String, RxConsumerTests.LogMessage>(
                        TimeSpan.FromSeconds(5), ThreadPoolScheduler.Instance)
                        .Take(10)
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
                        },
                            () => Console.WriteLine("COMPLETED.") // not sure how to cancel this...
                        );

                    Thread.Sleep(15000);
                    Console.WriteLine("Disposing observer");
                    observable.Dispose();
                }
                finally
                {
                    if (consumerInstance != null)
                    {
                        Console.WriteLine("Destroying instance");
                        client.DeleteConsumerAsync(consumerInstance);
                    }
                }
            }

            // Assert
            //Assert.Fail("Not Implemented Yet");

        }

    }

}

