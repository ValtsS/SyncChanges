using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace SyncChanges
{
    class ResultsBuffer
    {
        readonly string[] columns;
        readonly Queue<object[]> lines = new();

        public ResultsBuffer(string[] columns)
        {
            this.columns = columns;
        }

        public void ReadLine(DbDataReader reader)
        {
            object[] data = new object[columns.Length];
            for (int i = 0; i < data.Length; i++)
                data[i] = reader.GetValue(i);

            lines.Enqueue(data);

        }

        public string[] Columns => columns;

        public int Count => lines.Count;

        public Queue<object[]> Lines => lines;
    }
}
