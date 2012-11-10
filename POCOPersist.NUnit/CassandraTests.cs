using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using POCOPersist.Cassandra;
using POCOPersist.NUnit.TestPOCOs;

namespace POCOPersist.NUnit
{
    [TestFixture]
    public class CassandraTests
    {
        [SetUp]
        public void Init()
        {
            var client = new CassandraTypedClient<User>("Test", "User");
            client.CheckAndCreateKeyspaceAndColumnFamily();
        }

        [Test]
        public void C_InsertNewRecordById()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 1,
                Name = "Bob",
                Logins = 1
            };
            var u2In = new User()
            {
                Id = 2,
                Name = "Tim",
                Logins = 4
            };

            client.SetValue(u1In.Id, u1In);
            client.SetValue(u2In.Id, u2In);

            var u1Out = client.GetValue(1);
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            var u2Out = client.GetValue(2);
            Assert.AreEqual(u2In.Id, u2Out.Id);
            Assert.AreEqual(u2In.Name, u2Out.Name);
        }

        [Test]
        public void C_InsertNewRecordByStringKey()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 3,
                Name = "Tom",
                Logins = 1
            };
            var u2In = new User()
            {
                Id = 4,
                Name = "Fred",
                Logins = 4
            };

            client.SetValue("STRKEY-A", u1In);
            client.SetValue("STRKEY-B", u2In);

            var u1Out = client.GetValue("STRKEY-A");
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            var u2Out = client.GetValue("STRKEY-B");
            Assert.AreEqual(u2In.Id, u2Out.Id);
            Assert.AreEqual(u2In.Name, u2Out.Name);
        }

        [Test]
        public void C_InsertNewRecordByStringKeyLong()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 5,
                Name = "N34terfddf",
                Logins = 1
            };

            string key = "This is a really long and pointless key designed to trip up the engine.";
            client.SetValue(key, u1In);

            var u1Out = client.GetValue(key);
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);
        }

        [Test]
        public void C_InsertNewRecordByStringKey2D()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 6,
                Name = "Jethrow",
                Logins = 1
            };
            var u2In = new User()
            {
                Id = 7,
                Name = "Mathew",
                Logins = 4
            };

            client.SetValue("STRKEY-D", "A", u1In);
            client.SetValue("STRKEY-D", "B", u2In);

            var u1Out = client.GetValue("STRKEY-D", "A");
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            var u2Out = client.GetValue("STRKEY-D", "B");
            Assert.AreEqual(u2In.Id, u2Out.Id);
            Assert.AreEqual(u2In.Name, u2Out.Name);
        }

        [Test]
        public void C_InsertNewRecordByIntKey2D()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 8,
                Name = "Tommy",
                Logins = 1
            };
            var u2In = new User()
            {
                Id = 9,
                Name = "Burt",
                Logins = 4
            };

            client.SetValue(1, 1, u1In);
            client.SetValue(2, 1, u2In);

            var u1Out = client.GetValue(1, 1);
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            var u2Out = client.GetValue(2, 1);
            Assert.AreEqual(u2In.Id, u2Out.Id);
            Assert.AreEqual(u2In.Name, u2Out.Name);
        }

        [Test]
        public void C_DeleteKeyString()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 90001,
                Name = "DeleteMe",
                Logins = 1
            };

            client.SetValue("DeleteMe", u1In);

            var u1Out = client.GetValue("DeleteMe");
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            client.Remove("DeleteMe");
            u1Out = client.GetValue("DeleteMe");
            Assert.IsNull(u1Out);
        }

        [Test]
        public void C_DeleteKeyInt()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            var u1In = new User()
            {
                Id = 9,
                Name = "DeleteMe",
                Logins = 1
            };

            client.SetValue(u1In.Id, u1In);

            var u1Out = client.GetValue(u1In.Id);
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            client.Remove(u1In.Id);
            u1Out = client.GetValue(u1In.Id);
            Assert.IsNull(u1Out);
        }

        [Test]
        public void C_InsertNullObject()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            client.SetValue(4, null);
            client.SetValue(5, null);
            client.SetValue(1, 1, null);
            client.SetValue(2, 1, null);

            //these keys do not exist - We do not want exceptions
            client.SetValue(2, 17, null);
            client.SetValue(44, 0, null);

            var u1Out = client.GetValue(4);
            var u2Out = client.GetValue(5);
            var u3Out = client.GetValue(1, 1);
            var u4Out = client.GetValue(2, 1);

            Assert.IsNull(u1Out);
            Assert.IsNull(u2Out);
            Assert.IsNull(u3Out);
            Assert.IsNull(u4Out);
        }

        [Test]
        public void C_GetColumnsInt()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            client.SetValue(13, 1, new User() { Name = "Tim" });
            client.SetValue(13, 2, new User() { Name = "Fred" });

            var optCols = client.GetColumns<int, int>(13);

            var optItem = client.GetValue(13, optCols.ElementAt(0).ColumnName);
            var optItem2 = client.GetValue(13, optCols.ElementAt(1).ColumnName);

            Assert.AreEqual("Tim", optItem.Name);
            Assert.AreEqual("Fred", optItem2.Name);
        }

        [Test]
        public void C_GetColumnsString()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            client.SetValue("F-STRKEY", "One", new User() { Name = "Tim" });
            client.SetValue("F-STRKEY", "Two", new User() { Name = "Fred" });

            var optCols = client.GetColumns<string, string>("F-STRKEY");

            var optItem = client.GetValue("F-STRKEY", optCols.ElementAt(0).ColumnName);
            var optItem2 = client.GetValue("F-STRKEY", optCols.ElementAt(1).ColumnName);

            Assert.AreEqual("Tim", optItem.Name);
            Assert.AreEqual("Fred", optItem2.Name);
        }

        [Test]
        public void C_GetColumnValues()
        {
            var client = new CassandraTypedClient<User>("Test", "User");

            client.SetValue(12, 1, new User() { Name = "Bob" });
            client.SetValue(12, 2, new User() { Name = "Bob2" });
            client.SetValue(12, 3, new User() { Name = "Bob3" });
            client.SetValue(12, 4, new User() { Name = "Bob4" });

            var optCols = client.GetValues(12);

            Assert.AreEqual(4, optCols.Count());
        }

        [Test]
        public void C_ExpiringKeys()
        {
            var client = new CassandraTypedClient<User>("Test", "User");
            var i1 = new User() { Name = "Expire In 2", Id = 1 };
            var i2 = new User() { Name = "Expire In 4", Id = 2 };
            client.SetValue(12, i1.Id, i2, 2);
            client.SetValue(12, i2.Id, i2, 4);

            Thread.Sleep(1500);

            var o1 = client.GetValue(12, i1.Id);
            var o2 = client.GetValue(12, i2.Id);

            Assert.IsNotNull(o1);
            Assert.IsNotNull(o2);

            Thread.Sleep(1500);

            o1 = client.GetValue(12, i1.Id);
            o2 = client.GetValue(12, i2.Id);

            Assert.IsNull(o1);
            Assert.IsNotNull(o2);

            Thread.Sleep(1500);

            o1 = client.GetValue(12, i1.Id);
            o2 = client.GetValue(12, i2.Id);

            Assert.IsNull(o1);
            Assert.IsNull(o2);
        }
    }
}
