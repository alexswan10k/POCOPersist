using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace POCOPersist.Cassandra
{
    public class Column<T>
    {
        public T ColumnName { get; set; }
        public DateTime TimeStamp { get; set; }
        public TimeSpan? TimeToLive { get; set; }
    }
}
