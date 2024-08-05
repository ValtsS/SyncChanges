using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Text;


namespace SyncChanges
{
    class ResultsBuffer
    {
        readonly string[] columns;
        readonly Queue<object[]> lines = new();
        private object[] nullTypeMap = null;

        public ResultsBuffer(string[] columns)
        {
            this.columns = columns;
        }

        public void ReadLine(DbDataReader reader)
        {
            if (this.nullTypeMap == null)
                this.nullTypeMap = reader.GetNullableTypeMap();

            object[] data = new object[columns.Length];
            for (int i = 0; i < data.Length; i++)
            {
                var val = reader.GetValue(i);

                if (val == DBNull.Value)
                    data[i] = nullTypeMap[i] ?? val;
                else
                    data[i] = val;
            }


            lines.Enqueue(data);

        }

        public string[] Columns => columns;

        public int Count => lines.Count;

        public Queue<object[]> Lines => lines;
    }
}
