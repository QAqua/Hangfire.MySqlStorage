using System;
using System.Linq;
using System.Threading;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class ExpirationManagerTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly CancellationToken _token;

        public ExpirationManagerTests()
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection
                    .Execute(
                        "insert into HangfireAggregatedCounter (`Key`, Value, ExpireAt) values ('key', 1, @expireAt)", 
                        new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HangfireCounter").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into HangfireJob (InvocationData, Arguments, CreatedAt, ExpireAt) " +
                    "values ('', '', UTC_TIMESTAMP(), @expireAt)", 
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HangfireJob").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into HangfireList (`Key`, ExpireAt) values ('key', @expireAt)", 
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HangfireList").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into `HangfireSet` (`Key`, Score, Value, ExpireAt) values ('key', 0, '', @expireAt)", 
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from `HangfireSet`").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                const string createSql = @"
insert into HangfireHash (`Key`, Field, Value, ExpireAt) 
values ('key1', 'field', '', @expireAt),
       ('key2', 'field', '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HangfireHash").Single());
            }
        }

        private static int CreateExpirationEntry(MySqlConnection connection, DateTime? expireAt)
        {
            const string insertSql = @"
delete from HangfireAggregatedCounter;
insert into HangfireAggregatedCounter (`Key`, Value, ExpireAt)
values ('key', 1, @expireAt);
select last_insert_id() as Id";

            var id = connection.Query(insertSql, new { @expireAt = expireAt }).Single();
            var recordId = (int) id.Id;
            return recordId;
        }

        private static bool IsEntryExpired(MySqlConnection connection, int entryId)
        {
            var count = connection.Query<int>(
                    "select count(*) from HangfireAggregatedCounter where Id = @id", new { id = entryId }).Single();
            return count == 0;
        }

        private MySqlConnection CreateConnection()
        {
            return ConnectionUtils.CreateConnection();
        }

        private ExpirationManager CreateManager(MySqlConnection connection)
        {
            var storage = new MySqlStorage(connection);
            return new ExpirationManager(storage, TimeSpan.Zero);
        }
    }
}
