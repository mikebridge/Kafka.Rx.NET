using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Schema;
using Confluent.RestClient;
using Confluent.RestClient.Model;
using Moq;
using NUnit.Framework;

namespace Kafka.Rx.NET.Tests
{
    [TestFixture]
    public class RxConsumerTests
    {
        /*
        [Test]
        public void It_Should_Aggregate_The_Response_Into_An_Observable()
        {
            // Arrange
            var payload = new List<AvroMessage<string, LogMessage>>
            {
                new AvroMessage<string, LogMessage>(),
                new AvroMessage<string, LogMessage>()
            };

            var mockResponse = ConfluentResponse<List<AvroMessage<string, LogMessage>>>.Success(payload);
            var client = new Mock<IConfluentClient>();
            var consumerInstance = new ConsumerInstance();
            client.Setup(x => x.ConsumeAsAvroAsync<String, LogMessage>(It.IsAny<ConsumerInstance>(), "testtopic", 1024))
                .ReturnsAsync(mockResponse);

            //mockConsumerInstance.Setup(x => x.)
            var consumer = new RxConsumer(client.Object, consumerInstance, "testtopic");


            // Act
            var result = new List<Record<String, LogMessage>>();
            using (var stream = consumer.GetRecordStream<String, LogMessage>()
                .Subscribe(x => result.Add(x)))
            {
                Console.WriteLine("...");
            }
            
            // Assert
            Console.WriteLine("found "+result.Count + " results ");
            Assert.That(result.Count, Is.EqualTo(payload.Count));

        }
        */


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
