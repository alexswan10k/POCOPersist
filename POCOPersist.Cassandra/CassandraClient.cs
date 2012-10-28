using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra;
using ServiceStack.Text;
using POCOPersist.Core;

namespace POCOPersist.Cassandra
{
    public class CassandraClient : ClientBase
    {
        public CassandraClient()
        {
            Host = "127.0.0.1";
            HostPort = 9160;
            HostTimeout = 0;
            Keyspace = "Default";
        }

        public string Keyspace { get; set; }
        public string ColumnFamily { get; set; }
        public string Host { get; set; }
        public int HostPort { get; set; }
        public int HostTimeout { get; set; }

        public bool CheckKeyspaceAndColumnFamilyBeforeWrite { get; set; }

        public CassandraTypedClient<T> As<T>()
        {
            return new CassandraTypedClient<T>();
        }

        private const string ValueColumnIdentifier = "Val";

        bool keyspaceAndColumnFamilyChecked = false;
        public void CheckAndCreateKeyspaceAndColumnFamily(CassandraContext context)
        {
            keyspaceAndColumnFamilyChecked = false;
            checkAndCreateKeyspaceAndColumnFamily(context);
        }

        private void checkAndCreateKeyspaceAndColumnFamily(CassandraContext context)
        {
            if (CheckKeyspaceAndColumnFamilyBeforeWrite && !keyspaceAndColumnFamilyChecked)
            {
                if (!context.KeyspaceExists(Keyspace))
                    context.Keyspace.TryCreateSelf();

                if (!context.ColumnFamilyExists(ColumnFamily))
                    context.AddColumnFamily(new Apache.Cassandra.CfDef() { Name = ColumnFamily, Keyspace = Keyspace });
            }
        }

        protected string GetRawValue(string key)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    var col = colFamily.GetColumn(key, ValueColumnIdentifier);

                    if (col != null)
                        value = col.ColumnValue;
                    else
                        value = null;
                }
                catch(CassandraException ex)
                {
                    value = null;
                }
            }

            return value;
        }

        protected bool SetRawValue(string key, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (value != null)
                    colFamily.InsertColumn(key, ValueColumnIdentifier, value);
                else
                    colFamily.RemoveColumn(key, ValueColumnIdentifier);
            }

            return true;
        }

        protected string GetRawValue(int key)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    var col = colFamily.GetColumn(key, ValueColumnIdentifier);

                    if (col != null)
                        value = col.ColumnValue;
                    else
                        value = null;
                }
                catch(CassandraException ex)
                {
                    value = null;
                }
            }

            return value;
        }

        protected bool SetRawValue(int key, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if(value != null)
                    colFamily.InsertColumn(key, ValueColumnIdentifier, value);
                else
                    colFamily.RemoveColumn(key, ValueColumnIdentifier);
            }

            return true;
        }

        protected string GetRawValue(string key1, string key2)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    var col = colFamily.GetColumn(key1, key2);

                    if (col != null)
                        value = col.ColumnValue;
                    else
                        value = null;
                }
                catch(CassandraException ex)
                {
                    value = null;
                }
            }

            return value;
        }

        protected bool SetRawValue(string key1, string key2, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (value != null)
                    colFamily.InsertColumn(key1, key2, value);
                else
                    colFamily.RemoveColumn(key1, key2);
            }

            return true;
        }

        protected string GetRawValue(int key1, int key2)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    var col = colFamily.GetColumn(key1, key2);

                    if (col != null)
                        value = col.ColumnValue;
                    else
                        value = null;
                }
                catch (CassandraException ex)
                {
                    value = null;
                }
            }

            return value;
        }

        protected string GetRawValue(string key1, int key2)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);

                try
                {
                    var col = colFamily.GetColumn(key1, key2);

                    if (col != null)
                        value = col.ColumnValue;
                    else
                        value = null;
                }
                catch (CassandraException ex)
                {
                    value = null;
                }
            }

            return value;
        }

        protected string GetRawValue(int key1, string key2)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);

                try
                {
                    var col = colFamily.GetColumn(key1, key2);

                    if (col != null)
                        value = col.ColumnValue;
                    else
                        value = null;
                }
                catch (CassandraException ex)
                {
                    value = null;
                }
            }

            return value;
        }

        protected bool SetRawValue(int key1, int key2, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (value != null)
                    colFamily.InsertColumn(key1, key2, value);
                else
                    colFamily.RemoveColumn(key1, key2);
            }

            return true;
        }

        protected bool SetRawValue(string key1, int key2, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (value != null)
                    colFamily.InsertColumn(key1, key2, value);
                else
                    colFamily.RemoveColumn(key1, key2);
            }

            return true;
        }

        protected bool SetRawValue(int key1, string key2, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (value != null)
                    colFamily.InsertColumn(key1, key2, value);
                else
                    colFamily.RemoveColumn(key1, key2);
            }

            return true;
        }

        public void Remove(string key)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                colFamily.RemoveKey(key);
            }
        }

        public void Remove(int key)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                colFamily.RemoveKey(key);
            }
        }
    }
}
