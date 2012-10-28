POCOPersist is a simple library inspired by the great work done with ServiceStack.Redis to allow persisting POCO's to a data store as serialised JSON. 

The intention is to be able to write simple code like the code below to be able to retrieve and work with objects by a key.

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


The idea is that the interface resembles some of the simplest key/value operations available from the ServiceStack.Redis data access tools, and allows out of the box working with POCO's with minimal editing

The dependencies are as follows:
ServiceStack.Text for their JSOn serialiser
Nunit for unit tests
FluentCassandra for working with the Thrift API and higher level abstractions (possibly this should be replaced with a specific Thrift implementation at a later date)

Currently only Cassandra is implemented.