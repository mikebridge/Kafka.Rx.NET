using System;
using NUnit.Framework;

namespace Kafka.Rx.NET.Tests
{
    [TestFixture]
    public class BinaryConvertersTests
    {
        [Test]
        public void It_Should_Convert_To_Binary()
        {
            // Act
            String result = BinaryConverters.ToBinaryJson(new TestJson { Name = "test", Value = 99 });
            Console.WriteLine(result);
            // Assert
            Assert.That(result, Is.EqualTo("eyJOYW1lIjoidGVzdCIsIlZhbHVlIjo5OX0="));
        }

        [Test]
        public void It_Should_Convert_From_Binary()
        {
            // Act
            String base64 = "eyJOYW1lIjoidGVzdCIsIlZhbHVlIjo5OX0=";
            var result = BinaryConverters.FromBinaryJson<TestJson>(base64);
            // Assert
            Assert.That(result.Name, Is.EqualTo("test"));
            Assert.That(result.Value, Is.EqualTo(99));
        }


        public class TestJson
        {
            public String Name { get; set; }
            public int Value { get; set; }
        }

    }
}
