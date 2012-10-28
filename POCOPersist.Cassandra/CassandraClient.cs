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

        public static string ValueColumnIdentifier = "Val";

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
        #region GetRawValue


        protected string GetRawValue<T>(T key)
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    FluentColumn col = null;

                    if (key is string)
                        col = colFamily.GetColumn(key as string, ValueColumnIdentifier);
                    else if (key is int)
                        col = colFamily.GetColumn(key as int?, ValueColumnIdentifier);
                    else
                        throw new InvalidOperationException("Types not supported");

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

        protected string GetRawValue<T, U>(T key1, U key2) 
        {
            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    FluentColumn col = null;
                    if (key1 is string && key2 is string)
                        col = colFamily.GetColumn(key1 as string, key2 as string);
                    else if (key1 is string && key2 is int)
                        col = colFamily.GetColumn(key1 as string, key2 as int?);
                    else if (key1 is int && key2 is string)
                        col = colFamily.GetColumn(key1 as int?, key2 as string);
                    else if (key1 is int && key2 is int)
                        col = colFamily.GetColumn(key1 as int?, key2 as int?);
                    else
                        throw new InvalidOperationException("Types not supported");

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

        
        #endregion
        #region SetRawValue

        //protected bool SetRawValue<T>(T key, string value, int? timeToLive = null, DateTimeOffset offset = new DateTimeOffset())
        //{
        //    using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
        //    {
        //        checkAndCreateKeyspaceAndColumnFamily(context);
        //        var colFamily = context.GetColumnFamily(ColumnFamily);
        //        if (key is string)
        //        {
        //            if (value != null)
        //                colFamily.InsertColumn(key as string, ValueColumnIdentifier, value, offset, timeToLive);
        //            else
        //                colFamily.RemoveColumn(key as string, ValueColumnIdentifier);
        //        }
        //        else if (key is int)
        //        {
        //            if (value != null)
        //                colFamily.InsertColumn(key as int?, ValueColumnIdentifier, value, offset, timeToLive);
        //            else
        //                colFamily.RemoveColumn(key as int?, ValueColumnIdentifier);
        //        }
        //        else
        //            throw new InvalidOperationException("Types not supported");
        //    }

        //    return true;
        //}

        protected bool SetRawValue<T>(T key, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (key is string)
                {
                    if (value != null)
                        colFamily.InsertColumn(key as string, ValueColumnIdentifier, value);
                    else
                        colFamily.RemoveColumn(key as string, ValueColumnIdentifier);
                }
                else if (key is int)
                {
                    if (value != null)
                        colFamily.InsertColumn(key as int?, ValueColumnIdentifier, value);
                    else
                        colFamily.RemoveColumn(key as int?, ValueColumnIdentifier);
                }
                else
                    throw new InvalidOperationException("Types not supported");
            }

            return true;
        }

        protected bool SetRawValue<T, U>(T key1, U key2, string value)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (key1 is string && key2 is string)
                {
                    if (value != null)
                        colFamily.InsertColumn(key1 as string, key2 as string, value);
                    else
                        colFamily.RemoveColumn(key1 as string, key2 as string);
                }
                else if (key1 is string && key2 is int)
                {
                    if (value != null)
                        colFamily.InsertColumn(key1 as string, key2 as int?, value);
                    else
                        colFamily.RemoveColumn(key1 as string, key2 as int?);
                }
                else if (key1 is int && key2 is string)
                {
                    if (value != null)
                        colFamily.InsertColumn(key1 as int?, key2 as string, value);
                    else
                        colFamily.RemoveColumn(key1 as int?, key2 as string);
                }
                else if (key1 is int && key2 is int)
                {
                    if (value != null)
                        colFamily.InsertColumn(key1 as int?, key2 as int?, value);
                    else
                        colFamily.RemoveColumn(key1 as int?, key2 as int?);
                }
                else
                    throw new InvalidOperationException("Types not supported");
            }

            return true;
        }


        #endregion

        #region Remove

        public void Remove<T>(T key)
        {
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if(key is string)
                    colFamily.RemoveKey(key as string);
                else if (key is int)
                    colFamily.RemoveKey(key as int?);
                else
                    throw new InvalidOperationException("Types not supported");
            }
        }

        #endregion
    }
}
