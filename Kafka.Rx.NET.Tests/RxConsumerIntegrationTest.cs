using System;
using System.Reactive.Concurrency;
using System.Threading;
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
        public void KafkaObservable_Should_Provide_An_Observable_Stream()
        {
            // Arrange
            String instanceId = "0";
            string consumerGroupName = "rxtestgroup";
            string topic = "rxtest";
            ConsumerInstance consumerInstance = null;

            using (var client = RxConsumer.CreateConfluentClient("http://192.168.79.137:8082"))
            {
                try
                {
                    // in production this should be written without blocking.
                    var consumerInstanceTask = RxConsumer.ConsumerInstance(client, instanceId, consumerGroupName).Result;
                    if (!consumerInstanceTask.IsSuccess())
                    {
                        Assert.Fail("Error " + consumerInstanceTask.Error.ErrorCode + ": " +
                                    consumerInstanceTask.Error.Message);
                    }
                    consumerInstance = consumerInstanceTask.Payload;
                    var consumer = new RxConsumer(client, consumerInstance, topic);

                    // Act
                    var observable = consumer.GetRecordStream<String, RxConsumerTests.LogMessage>(
                        TimeSpan.FromSeconds(5), ThreadPoolScheduler.Instance)
                        
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
            // Act

            // Assert
            //Assert.Fail("Not Implemented Yet");

        }

        

//        [Test]
//        public void KafkaObservable_Should_Provide_An_Observable_Stream()
//        {
//            var conn = new RxConsumer("test", "test");
//            ValueSourceAttribute messages = GetFakeKafkaMessages();
//            var stream = conn.GetObservableStream(messages);
//            var list = stream.ToBlocking
//        }
//
//        private IEnumerable<MessageAndMetaData<byte[], byte[]>> GetFakeKafkaMessages(int numMessages)
//        {
//            return new IEnumerable<MessageAndMetaData<byte[], byte[]>>();
////           val decoder = new DefaultDecoder()
////    (0 to numMessages - 1) map { num =>
////      val rawMessage = new kafka.message.Message(num.toString.getBytes)
//      MessageAndMetadata(topic = num.toString, partition = num, offset = num.toLong, rawMessage = rawMessage, keyDecoder = decoder, valueDecoder = decoder)
//    }
//  }
        }

    }

