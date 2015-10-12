
namespace Kafka.Rx.NET
{
    public class Record<TK,TV>
    {
        private readonly TK _key;
        private readonly TV _value;

        public Record(TK key, TV value)
        {
            _key = key;
            _value = value;
        }

        public TK Key { get { return _key; } }

        public TV Value { get { return _value; } }
    }
}
