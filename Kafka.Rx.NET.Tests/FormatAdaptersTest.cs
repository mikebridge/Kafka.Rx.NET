using System;
using System.Collections.Generic;
using System.Linq;

using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;
using Moq;
using NUnit.Framework;

namespace Kafka.Rx.NET.Tests
{
    [TestFixture]
    public class FormatAdaptersTest
    {
        [Test]
        public void It_Should_Convert_An_Avro_Record()
        {
            var payload = new TestJson { Name = "Test Name", Value = 99 };
            var mockPayload = CreateAvroMessageList(new List<TestJson> { payload });
            var client = new Mock<IConfluentClient>();

            client.Setup(x => x.ConsumeAsAvroAsync<String, TestJson>(It.IsAny<ConsumerInstance>(), "testtopic", It.IsAny<int?>()))
                .Returns(Task.FromResult(ConfluentResponse<List<AvroMessage<String, TestJson>>>.Success(mockPayload.ToList())));

            var consumerInstance = new ConsumerInstance();

            // Act
            var result = FormatAdapters.ConsumeOnceAsAvroAsync<String,TestJson>(
                client.Object,
                consumerInstance,
                "testtopic").Result;

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var records = result.Value.ToList();
            Assert.That(records[0].Value.Name, Is.EqualTo(payload.Name));
            Assert.That(records[0].Value.Value, Is.EqualTo(payload.Value));

        }


        [Test]
        public void It_Should_Convert_A_Binary_Record()
        {
            // Arrange
            var payload = new TestJson { Name = "Test Name", Value = 99 };
            var mockPayload = CreateBinaryMessageList(new List<TestJson>{payload});
            var client = new Mock<IConfluentClient>();

            client.Setup(x => x.ConsumeAsBinaryAsync(It.IsAny<ConsumerInstance>(), "testtopic", It.IsAny<int?>()))
                .Returns(Task.FromResult(ConfluentResponse<List<BinaryMessage>>.Success(mockPayload.ToList())));

            var consumerInstance = new ConsumerInstance();

            // Act
            var result = FormatAdapters.ConsumeOnceAsBinaryAsync<String, TestJson>(
                client.Object, 
                consumerInstance,
                "testtopic").Result;
            
            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var records = result.Value.ToList();
            Assert.That(records[0].Value.Name, Is.EqualTo(payload.Name));
            Assert.That(records[0].Value.Value, Is.EqualTo(payload.Value));


        }

        private static IEnumerable<BinaryMessage> CreateBinaryMessageList(IEnumerable<TestJson> payload)
        {            
            return payload.Select((value, index) => new BinaryMessage
            {
                Key=null, 
                Offset=index, 
                Partition=0, 
                Value=BinaryConverters.ToBinaryJson(value)
            });
        }

        private static IEnumerable<AvroMessage<String,TestJson>> CreateAvroMessageList(IEnumerable<TestJson> payload)
        {
            return payload.Select((value, index) => new AvroMessage<String, TestJson>
            {
                Key = null,
                Offset = index,
                Partition = 0,
                Value = value
            });
        }



        public class TestJson
        {
            public String Name { get; set; }
            public int Value { get; set; }
        }

    }
}
