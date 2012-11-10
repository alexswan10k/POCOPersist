
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

        public T GetValue(string key, string column)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key, column));
        }

        public T GetValue(string key, int column)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key, column));
        }

        public T GetValue(int key, string column)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key, column));
        }
        public T GetValue(int key, int column)
        {
            return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key, column));
        }
        #endregion

        #region SetValue
        public void SetValue(string key, T value, int? timeToLive = null, DateTimeOffset? offset = null)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, serialisedObject, timeToLive, offset);
        }

        public void SetValue(int key, T value, int? timeToLive = null, DateTimeOffset? offset = null)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, serialisedObject, timeToLive, offset);
        }

        public void SetValue(string key, string column, T value, int? timeToLive = null, DateTimeOffset? offset = null)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, column, serialisedObject, timeToLive, offset);
        }

        public void SetValue(string key, int column, T value, int? timeToLive = null, DateTimeOffset? offset = null)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, column, serialisedObject, timeToLive, offset);
        }

        public void SetValue(int key, string column, T value, int? timeToLive = null, DateTimeOffset? offset = null)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, column, serialisedObject, timeToLive, offset);
        }
        public void SetValue(int key, int column, T value, int? timeToLive = null, DateTimeOffset? offset = null)
        {
            var serialisedObject = ServiceStack.Text.JsonSerializer.SerializeToString<T>(value);
            SetRawValue(key, column, serialisedObject, timeToLive, offset);
        }
        #endregion

        public IEnumerable<T> GetValues(string key)
        {
            var outList = new List<T>();
            foreach (var value in this.GetRawValues(key))
            {
                T obj = ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(this.GetRawValue(key));

                if (obj != null)
                    outList.Add(obj);
            }

            return outList;
        }

        public IEnumerable<T> GetValues(int key)
        {
            var outList = new List<T>();
            foreach (var value in this.GetRawValues(key))
            {
                T obj = ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(value);

                if (obj != null)
                    outList.Add(obj);
            }

            return outList;
        }

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
