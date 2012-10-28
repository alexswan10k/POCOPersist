
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra;
using POCOPersist.Core;

namespace POCOPersist.Cassandra
{
    public class CassandraTypedClient<T> : CassandraClient, TypedClientBase<T>
    {
        public CassandraTypedClient() { }

        public CassandraTypedClient(string keyspace, string columnFamily)
        {
            Keyspace = keyspace;
            ColumnFamily = columnFamily;
        }

        public T GetValue(string key)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key));
        }

        public void SetValue(string key, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, serialisedObject);
        }

        public T GetValue(int key)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key));
        }

        public void SetValue(int key, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, serialisedObject);
        }

        public T GetValue(string key1, string key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }

        public void SetValue(string key1, string key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }

        public T GetValue(int key1, int key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }

        public void SetValue(int key1, int key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }

        public T GetValue(string key1, int key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }

        public void SetValue(string key1, int key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }

        public T GetValue(int key1, string key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }

        public void SetValue(int key1, string key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }

        public T this[string key]
        {
            get
            {
                return GetValue(key);
            }

            set
            {
                SetValue(key, value);
            }
        }
    }
}
