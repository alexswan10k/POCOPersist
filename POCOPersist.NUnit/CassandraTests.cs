using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using POCOPersist.Cassandra;
using POCOPersist.NUnit.TestPOCOs;

namespace POCOPersist.NUnit
{
    [TestFixture]
    public class CassandraTests
    {
        [Test]
        public void C_InsertNewRecordById()
        {
            var client = new CassandraTypedClient<User>("Test", "User");
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

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
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

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

            client.SetValue("A", u1In);
            client.SetValue("B", u2In);

            var u1Out = client.GetValue("A");
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            var u2Out = client.GetValue("B");
            Assert.AreEqual(u2In.Id, u2Out.Id);
            Assert.AreEqual(u2In.Name, u2Out.Name);
        }

        [Test]
        public void C_InsertNewRecordByStringKeyLong()
        {
            var client = new CassandraTypedClient<User>("Test", "User");
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

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
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

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

            client.SetValue("D", "A", u1In);
            client.SetValue("D", "B", u2In);

            var u1Out = client.GetValue("D", "A");
            Assert.AreEqual(u1In.Id, u1Out.Id);
            Assert.AreEqual(u1In.Name, u1Out.Name);

            var u2Out = client.GetValue("D", "B");
            Assert.AreEqual(u2In.Id, u2Out.Id);
            Assert.AreEqual(u2In.Name, u2Out.Name);
        }

        [Test]
        public void C_InsertNewRecordByIntKey2D()
        {
            var client = new CassandraTypedClient<User>("Test", "User");
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

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
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

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
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

            var u1In = new User()
            {
                Id = 90001,
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
            client.CheckKeyspaceAndColumnFamilyBeforeWrite = true;

            client.SetValue(4, null);
            client.SetValue(5, null);
            client.SetValue(1, 1, null);
            client.SetValue(2, 1, null);

            var u1Out = client.GetValue(4);
            var u2Out = client.GetValue(5);
            var u3Out = client.GetValue(1, 1);
            var u4Out = client.GetValue(2, 1);

            Assert.IsNull(u1Out);
            Assert.IsNull(u2Out);
            Assert.IsNull(u3Out);
            Assert.IsNull(u4Out);
        }
    }
}
