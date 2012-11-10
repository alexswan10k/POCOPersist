using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra;
using ServiceStack.Text;
using POCOPersist.Core;
using FluentCassandra.Types;

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

        public static int ValueColumnIdentifier = 0;

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

        public void CheckAndCreateKeyspaceAndColumnFamily()
        {
            CheckKeyspaceAndColumnFamilyBeforeWrite = true;
            keyspaceAndColumnFamilyChecked = false;
            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
            }
        }
        #region GetRawValue

        protected string GetRawValue<T>(T key)
        {
            if (key is string && (key as string).Count() < Key_String_Minimum_Size)
                throw new Exception(Key_Too_Short_Exception_Message);

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

        protected string GetRawValue<T, U>(T key, U column) 
        {
            if (key is string && (key as string).Count() < Key_String_Minimum_Size)
                throw new Exception(Key_Too_Short_Exception_Message);

            string value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    FluentColumn col = null;
                    if (key is string && column is string)
                        col = colFamily.GetColumn(key as string, column as string);
                    else if (key is string && column is int)
                        col = colFamily.GetColumn(key as string, column as int?);
                    else if (key is int && column is string)
                        col = colFamily.GetColumn(key as int?, column as string);
                    else if (key is int && column is int)
                        col = colFamily.GetColumn(key as int?, column as int?);
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

        protected IEnumerable<string> GetRawValues<T>(T key)
        {
            if (key is string && (key as string).Count() < Key_String_Minimum_Size)
                throw new Exception(Key_Too_Short_Exception_Message);

            IEnumerable<string> value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    IEnumerable<FluentColumn> cols = null;
                    CassandraObject[] keyToPass = null;

                    keyToPass = new CassandraObject[] { IntegerType.GetCassandraObjectFromObject(key, typeof(BytesType)) };

                    var rowz = colFamily.Get(keyToPass);

                    if (rowz != null)
                    {
                        cols = rowz.First().Columns;
                    }

                    if (cols != null)
                        value = cols.Select(s=>(string)s.ColumnValue);
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

        public IEnumerable<Column<U>> GetColumns<T, U>(T key)
        {
            if (key is string && (key as string).Count() < Key_String_Minimum_Size)
                throw new Exception(Key_Too_Short_Exception_Message);

            IEnumerable<Column<U>> value = null;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                var colFamily = context.GetColumnFamily(ColumnFamily);
                try
                {
                    IEnumerable<FluentColumn> cols = null;
                    CassandraObject[] keyToPass = null;

                    keyToPass = new CassandraObject[] { IntegerType.GetCassandraObjectFromObject(key, typeof(BytesType)) };

                    var rowz = colFamily.Get(keyToPass);

                    if (rowz != null)
                    {
                        cols = rowz.First().Columns;
                    }

                    if (cols != null)
                    {
                        var optVal = cols.Select(s => new { s.ColumnName, timeStamp = new DateTime(s.ColumnTimestamp.Ticks), s.ColumnTimeUntilDeleted });

                        if(typeof(U) == typeof(int))
                            value = optVal.Select(s => new Column<U>() { ColumnName = (U)(object)s.ColumnName.GetValue<int>(), TimeStamp = s.timeStamp, TimeToLive = s.ColumnTimeUntilDeleted } );
                        else if(typeof(U)== typeof( string))
                            value = optVal.Select(s => new Column<U>() { ColumnName = (U)(object)s.ColumnName.GetValue<string>(), TimeStamp = s.timeStamp, TimeToLive = s.ColumnTimeUntilDeleted });
                    }
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

        #region SetRawValue

        private const string Key_Too_Short_Exception_Message = "The key entered is too short. Keys must be at least 8 characters long so they do not step on numeric keys.";
        private const int Key_String_Minimum_Size = 8;

        protected bool SetRawValue<T>(T key, string value, int? timeToLiveSeconds = null, DateTimeOffset? offset = null)
        {
            if (offset == null)
                offset = DateTimeOffset.UtcNow;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (key is string)
                {
                    if ((key as string).Count() < Key_String_Minimum_Size)
                        throw new Exception(Key_Too_Short_Exception_Message);

                    if (value != null)
                    {
                        colFamily.InsertColumn(key as string, ValueColumnIdentifier, value, DateTimeOffset.Now, timeToLiveSeconds);
                    }
                    else
                        colFamily.RemoveColumn(key as string, ValueColumnIdentifier);
                }
                else if (key is int)
                {
                    if (value != null)
                        colFamily.InsertColumn(key as int?, ValueColumnIdentifier, value, DateTimeOffset.Now, timeToLiveSeconds);
                    else
                        colFamily.RemoveColumn(key as int?, ValueColumnIdentifier);
                }
                else
                    throw new InvalidOperationException("Types not supported");
            }

            return true;
        }

        protected bool SetRawValue<T, U>(T key, U column, string value, int? timeToLiveSeconds = null, DateTimeOffset? offset = null)
        {
            if (offset == null)
                offset = DateTimeOffset.UtcNow;

            using (var context = new CassandraContext(new CassandraSession(Keyspace, new FluentCassandra.Connections.Server(Host, HostPort, HostTimeout))))
            {
                checkAndCreateKeyspaceAndColumnFamily(context);
                var colFamily = context.GetColumnFamily(ColumnFamily);
                if (key is string && column is string)
                {
                    if((key as string).Count() < 8)
                        throw new Exception(Key_Too_Short_Exception_Message);

                    if (value != null)
                        colFamily.InsertColumn(key as string, column as string, value, DateTimeOffset.Now, timeToLiveSeconds);
                    else
                        colFamily.RemoveColumn(key as string, column as string);
                }
                else if (key is string && column is int)
                {
                    if((key as string).Count() < 8)
                        throw new Exception(Key_Too_Short_Exception_Message);

                    if (value != null)
                        colFamily.InsertColumn(key as string, column as int?, value, DateTimeOffset.Now, timeToLiveSeconds);
                    else
                        colFamily.RemoveColumn(key as string, column as int?);
                }
                else if (key is int && column is string)
                {
                    if (value != null)
                        colFamily.InsertColumn(key as int?, column as string, value, DateTimeOffset.Now, timeToLiveSeconds);
                    else
                        colFamily.RemoveColumn(key as int?, column as string);
                }
                else if (key is int && column is int)
                {
                    if (value != null)
                        colFamily.InsertColumn(key as int?, column as int?, value, DateTimeOffset.Now, timeToLiveSeconds);
                    else
                        colFamily.RemoveColumn(key as int?, column as int?);
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

                context.SaveChanges();
            }
        }

        #endregion
    }
}
