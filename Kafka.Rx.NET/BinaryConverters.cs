using System;
using System.Text;
using Newtonsoft.Json;

namespace Kafka.Rx.NET
{
    public static class BinaryConverters
    {
        public static T FromBinaryJson<T>(String binary)
        {
            //Console.WriteLine(binary);
            byte[] data = Convert.FromBase64String(binary);
            string decodedString = Encoding.UTF8.GetString(data);
            return JsonConvert.DeserializeObject<T>(decodedString);
        }

        public static String ToBinaryJson<T>(T obj)
        {
            String str = JsonConvert.SerializeObject(obj);
            var bytes = Encoding.UTF8.GetBytes(str);
            return Convert.ToBase64String(bytes);
        }
    }
}
