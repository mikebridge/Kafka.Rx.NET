using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.Serialization;
using System.Security.Policy;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Schema;
using Confluent.RestClient;
using Confluent.RestClient.Model;
using Microsoft.Reactive.Testing;
using Moq;
using NUnit.Framework;

namespace Kafka.Rx.NET.Tests
{
    [TestFixture]
    public class RxConsumerTests
    {
        private int _offset;

        [SetUp]
        public void Setup()
        {
            _offset = 0;
        }

        [Test]
        public void It_Should_Aggregate_The_Response_Into_An_Observable()
        {
            // Arrange
            int tickInterval = 1;
            var consumer = new RxConsumer(new Mock<IConfluentClient>().Object, new ConsumerInstance(), "testStream");
            var fn = MockDataTask();

            // Act
            var scheduler = new TestScheduler();

            var observable = consumer.GetRecordStream(fn, TimeSpan.FromTicks(tickInterval), scheduler); //.Take(10);

            var heardMessages = new List<LogMessage>();
            var exceptions = new List<Exception>();
            var subscription = observable.Subscribe(
                successResult =>
                {
                    Console.WriteLine("Success: " + successResult.IsSuccess);
                    if (successResult.IsSuccess)
                    {
                        Console.WriteLine(successResult.Value.Key + "=" + successResult.Value.Value.Message);
                        heardMessages.Add(successResult.Value.Value);
                    }
                    else
                    {
                        Console.WriteLine("ERROR: " + successResult.Exception.Message);
                        exceptions.Add(successResult.Exception);
                    }
                });


            Console.WriteLine("Start the scheduler");
            //scheduler.Start();
            scheduler.AdvanceBy(tickInterval);
            scheduler.AdvanceBy(tickInterval);
            Console.WriteLine("Running...");
            subscription.Dispose();
            
            // Assert
            Assert.That(exceptions, Has.Count.EqualTo(0));
            Assert.That(heardMessages, Has.Count.EqualTo(4));

        }

        private Func<IConfluentClient, ConsumerInstance, string, Task<ConfluentResponse<List<AvroMessage<string, LogMessage>>>>> MockDataTask()
        {
            Func
                <IConfluentClient, ConsumerInstance, String,
                    Task<ConfluentResponse<List<AvroMessage<String, LogMessage>>>>> fn =
                        (_1, _2, _3) =>
                        {
                            Console.WriteLine("Creating Test Task");
                            return Task.FromResult(TestResponse());
                        };
            return fn;
        }

        private long GetOffset()
        {
            return _offset ++;
        }

        private ConfluentResponse<List<AvroMessage<string, LogMessage>>> TestResponse()
        {
            // Arrange
            var payload = new List<AvroMessage<string, LogMessage>>
            {
                CreateResult(),
                CreateResult()
            };

            return ConfluentResponse<List<AvroMessage<string, LogMessage>>>.Success(payload);
        }

        private AvroMessage<string, LogMessage> CreateResult()
        {
            long offset = GetOffset();
            return new AvroMessage<string, LogMessage> { Key = "thekey", Offset = offset, Partition = 0, Value = new LogMessage { Message = "Test #" + offset } };
        }

//        [Test]
//        public void It_Should_Aggregate_The_Response_Into_An_Observable2()
//        {
//            // Arrange
//            var payload = new List<AvroMessage<string, LogMessage>>
//            {
//                new AvroMessage<string, LogMessage>(),
//                new AvroMessage<string, LogMessage>()
//            };
//
//            var mockResponse = ConfluentResponse<List<AvroMessage<string, LogMessage>>>.Success(payload);
//            var client = new Mock<IConfluentClient>();
//            var consumerInstance = new ConsumerInstance();
//            client.Setup(x => x.ConsumeAsAvroAsync<String, LogMessage>(It.IsAny<ConsumerInstance>(), "testtopic", 1024))
//                .ReturnsAsync(mockResponse);
//
//            //mockConsumerInstance.Setup(x => x.)
//            var consumer = new RxConsumer(client.Object, consumerInstance, "testtopic");
//
//
//            // Act
//            var result = new List<Record<String, LogMessage>>();
//            using (var stream = consumer.GetRecordStream<String, LogMessage>()
//                .Subscribe(x => result.Add(x)))
//            {
//                Console.WriteLine("...");
//            }
//            
//            // Assert
//            Console.WriteLine("found "+result.Count + " results ");
//            Assert.That(result.Count, Is.EqualTo(payload.Count));
//
//        }



        [DataContract]
        public class LogMessage
        {
            [DataMember(Name = "message")]
            public String Message { get; set; }

            [DataMember(Name = "stacktrace")]
            public String StackTrace { get; set; }
        }


    }
}
