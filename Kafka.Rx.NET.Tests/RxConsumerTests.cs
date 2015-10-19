using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.Serialization;
using System.Threading.Tasks;
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
        private const int TestSchedulerTickInterval = 1;
        private int _offset;

        [SetUp]
        public void Setup()
        {
            _offset = 0;
        }

        [Test]
        public void It_Should_Flatten_Lists_Of_Records_Into_An_Observable()
        {
            // Arrange
            var scheduler = new TestScheduler();
            var heardMessages = new List<LogMessage>();
            var exceptions = new List<Exception>();

            var subscription = CreateSubscription(
                scheduler,
                MockDataTask(CreateTestRecords(2)),
                msg => heardMessages.Add(msg), 
                ex => exceptions.Add(ex));

            // Act
            scheduler.AdvanceBy(TestSchedulerTickInterval);
            scheduler.AdvanceBy(TestSchedulerTickInterval);
            subscription.Dispose();
            
            // Assert
            Assert.That(exceptions, Has.Count.EqualTo(0));
            Assert.That(heardMessages, Has.Count.EqualTo(4));

        }

        [Test]
        public void It_Should_Process_An_Error_Observable()
        {
            // Arrange
            var scheduler = new TestScheduler();
            var heardMessages = new List<LogMessage>();
            var exceptions = new List<Exception>();

            var subscription = CreateSubscription(
                scheduler,
                MockDataTask(CreateErrorMessage(999, "Test Error")),
                msg => heardMessages.Add(msg),
                ex => exceptions.Add(ex));

            // Act
            scheduler.AdvanceBy(TestSchedulerTickInterval);
            scheduler.AdvanceBy(TestSchedulerTickInterval);
            subscription.Dispose();
            
            // Assert
            Assert.That(exceptions, Has.Count.EqualTo(2));
            Assert.That(heardMessages, Has.Count.EqualTo(0));

        }


        [Test]
        public void It_Should_Process_An_Exception_As_An_Error()
        {
            // Arrange
            var scheduler = new TestScheduler();
            var heardMessages = new List<LogMessage>();
            var exceptions = new List<Exception>();

            var subscription = CreateSubscription(
                scheduler,
                MockExceptionTask("Test Exception"),
                msg => heardMessages.Add(msg),
                ex => exceptions.Add(ex));

            // Act
            scheduler.AdvanceBy(TestSchedulerTickInterval);
            scheduler.AdvanceBy(TestSchedulerTickInterval);
            subscription.Dispose();

            // Assert
            Assert.That(exceptions, Has.Count.EqualTo(2));
            Assert.That(heardMessages, Has.Count.EqualTo(0));

        }

        private Func<IConfluentClient, ConsumerInstance, string, Task<Try<IEnumerable<Record<string, LogMessage>>>>>
            MockExceptionTask(string testException)
        {
            return (_1, _2, _3) => { throw new Exception(testException); };
        }


//        private Func<IConfluentClient, ConsumerInstance, string, Task<ConfluentResponse<List<AvroMessage<string, LogMessage>>>>> MockExceptionTask(string testException)
//        {
//            return (_1, _2, _3) => { throw new Exception(testException); };
//        }

        private IDisposable CreateSubscription(
            TestScheduler scheduler, 
            Func<IConfluentClient, ConsumerInstance, string, Task<Try<IEnumerable<Record<string, LogMessage>>>>> getPayload,
            Action<LogMessage> onSuccess,
            Action<Exception> onException)
        {
            var consumer = new RxConsumer<String, LogMessage>(new Mock<IConfluentClient>().Object, new ConsumerInstance(), "testStream");
            var observable = consumer.GetRecordStream(
                //FormatAdapters.ConsumeOnceAsAvroAsync<String,LogMessage>,
                getPayload, 
                TimeSpan.FromTicks(TestSchedulerTickInterval), 
                scheduler,
                () => { });

            var subscription = observable.Subscribe(
                successResult =>
                {
                    Console.WriteLine("Success: " + successResult.IsSuccess);
                    if (successResult.IsSuccess)
                    {
                        Console.WriteLine(successResult.Value.Key + "=" + successResult.Value.Value.Message);
                        onSuccess(successResult.Value.Value);
                    }
                    else
                    {
                        Console.WriteLine("ERROR: " + successResult.Exception.Message);
                        onException(successResult.Exception);
                    }
                });
            return subscription;
        }

        private Func<IConfluentClient, ConsumerInstance, string, Task<Try<IEnumerable<Record<string, LogMessage>>>>> MockDataTask(
            Try<IEnumerable<Record<string, LogMessage>>> confluentResponse)
        {
            return (_1, _2, _3) => Task.FromResult(confluentResponse);
        }

        private long GetOffset()
        {
            return _offset ++;
        }

        private Try<IEnumerable<Record<string, LogMessage>>> CreateErrorMessage(int i, string testError)
        {
            return new Failure<IEnumerable<Record<string, LogMessage>>>(new Exception("Test Error"));
        }

//        private ConfluentResponse<List<AvroMessage<string, LogMessage>>> CreateErrorMessage(int errorCode, string testError)
//        {
//            return ConfluentResponse<List<AvroMessage<string, LogMessage>>>.Failed(new Error { ErrorCode = errorCode, Message = testError });
//        }

        private Try<IEnumerable<Record<string, LogMessage>>> CreateTestRecords(int responseCount)
        {

            // Arrange
            var payload = Enumerable.Range(0, responseCount)
                .Select(_ => CreateResult());
            return new Success<IEnumerable<Record<string, LogMessage>>> (payload);
        }

//        private ConfluentResponse<List<AvroMessage<string, LogMessage>>> CreateTestRecords(int responseCount)
//        {
//            // Arrange
//            var payload = Enumerable.Range(0, responseCount)
//                                    .Select(_ => CreateResult())
//                                    .ToList();
//            return ConfluentResponse<List<AvroMessage<string, LogMessage>>>.Success(payload);
//        }

        private Record<string, LogMessage> CreateResult()
        {
            long offset = GetOffset();
            return new Record<string, LogMessage>(key: "thekey", value: new LogMessage {Message = "Test #" + offset});
            //long offset = GetOffset();
//            return new Record<string, LogMessage>(key: "thekey"
//            {
//                Key = "thekey", Offset = offset, Partition = 0, Value = new LogMessage { Message = "Test #" + offset }
//            };
        }


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
