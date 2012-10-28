
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
        public CassandraTypedClient() : base(){ }

        public CassandraTypedClient(string keyspace, string columnFamily) : base()
        {
            Keyspace = keyspace;
            ColumnFamily = columnFamily;
        }

        #region GetValue
        public T GetValue(string key)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key));
        }

        public T GetValue(int key)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key));
        }

        public T GetValue(string key1, string key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }

        public T GetValue(string key1, int key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }

        public T GetValue(int key1, string key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }
        public T GetValue(int key1, int key2)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key1, key2));
        }
        #endregion
        #region SetValue
        public void SetValue(string key, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, serialisedObject);
        }

        public void SetValue(int key, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, serialisedObject);
        }

        public void SetValue(string key1, string key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }

        public void SetValue(string key1, int key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }

        public void SetValue(int key1, string key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }
        public void SetValue(int key1, int key2, T value)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key1, key2, serialisedObject);
        }
        #endregion
        #region Indexers
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

        public T this[int key]
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
        #endregion
    }
}
