using Humanizer;
using Newtonsoft.Json;
using NLog;
using NPoco;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SyncChanges
{
    /// <summary>
    /// Allows replication of database changes from a source database to one or more destination databases.
    /// </summary>
    public class Synchronizer
    {
        /// <summary>
        /// Gets or sets a value indicating whether destination databases will be modified during a replication run.
        /// </summary>
        /// <value>
        ///   <c>true</c> if destination databases will be modified; otherwise, <c>false</c>.
        /// </value>
        public bool DryRun { get; set; } = false;

        /// <summary>
        /// Gets or sets the database connection timeout.
        /// </summary>
        /// <value>
        /// The database connection timeout.
        /// </value>
        public int Timeout { get; set; } = 0;

        /// <summary>
        /// Gets or sets the minimum synchronization time interval in seconds. Default is 30 seconds.
        /// </summary>
        /// <value>
        /// The synchronization time interval in seconds.
        /// </value>
        public int Interval { get; set; } = 30;

        /// <summary>
        /// Occurs when synchronization from a synchronization loop has succeeded.
        /// </summary>
        public event EventHandler<SyncEventArgs> Synced;

        static readonly Logger Log = LogManager.GetCurrentClassLogger();
        Config Config { get; set; }
        bool Error { get; set; }

        private static readonly Dictionary<string, string> PermissiveTypeMap = new Dictionary<string, string>
        {
            { "189;189;8;0;0;1;0", "173;173;8;0;0;1;0" },
            { "173;173;8;0;0;1;0", "189;189;8;0;0;1;0" },
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="Synchronizer"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <exception cref="System.ArgumentException"><paramref name="config"/> is null</exception>
        public Synchronizer(Config config)
        {
            Config = config ?? throw new ArgumentException("config is null", nameof(config));
        }

        private IList<IList<TableInfo>> Tables { get; } = new List<IList<TableInfo>>();
        private bool Initialized = false;

        /// <summary>
        /// Initialize the synchronization process.
        /// </summary>
        public void Init()
        {
            if (Timeout != 0)
                Log.Info($"Command timeout is {"second".ToQuantity(Timeout)}");

            for (int i = 0; i < Config.ReplicationSets.Count; i++)
            {
                var replicationSet = Config.ReplicationSets[i];

                Log.Info($"Getting replication information for replication set {replicationSet.Name}");

                var tables = GetTables(replicationSet.Source);
                if (replicationSet.Tables != null && replicationSet.Tables.Any())
                    tables = tables.Select(t => new { Table = t, Name = t.Name.Replace("[", "").Replace("]", "") })
                        .Where(t => replicationSet.Tables.Exists(r => r == t.Name || r == t.Name.Split('.')[1]))
                        .Select(t => t.Table).ToList();

                if (!tables.Any())
                    Log.Warn("No tables to replicate (check if change tracking is enabled)");
                else
                    Log.Info($"Replicating {"table".ToQuantity(tables.Count, ShowQuantityAs.None)} {string.Join(", ", tables.Select(t => t.Name))}");

                Tables.Add(tables);
            }

            Initialized = true;
        }


        Dictionary<string, TableInfo> MapTableInfo(IEnumerable<TableInfo> tables)
        {
            return tables.ToDictionary(t => t.Name.Replace("[", "").Replace("]", ""), t => t);
        }

        bool SchemaChanged(IList<TableInfo> tablesNew, IList<TableInfo> tables, IEnumerable<string> interestingTables)
        {

            var newtables = MapTableInfo(tablesNew);
            var oldtables = MapTableInfo(tables);

            foreach(var tableName in interestingTables)
            {
                if (newtables.TryGetValue(tableName, out TableInfo newTable) &&
                    oldtables.TryGetValue(tableName, out TableInfo oldTable))
                {

                    if (newTable.ColumnTypeDescriptions.Count != oldTable.ColumnTypeDescriptions.Count)
                    {
                        Log.Error($"Column count mismatch in table {tableName}");
                        return true;
                    }

                    bool mismatches = false;
                    foreach(var kv in oldTable.ColumnTypeDescriptions)
                    {
                        if (newTable.ColumnTypeDescriptions.TryGetValue(kv.Key, out string newDesc))
                        {
                            if (newDesc != kv.Value)
                            {
                                if (PermissiveTypeMap.TryGetValue(newDesc, out string alsoValid) && alsoValid == kv.Value)
                                    continue; // rowversion cannot be mapped exactly

                                Log.Error($"Column type mismatch in table {tableName} column {kv.Key}");
                                mismatches = true;
                            }
                        } else {
                            Log.Error($"Column {kv.Key} not found in table {tableName}");
                            mismatches = true;
                        }
                    }

                    return mismatches;

                } else
                {
                    Log.Error($"Could not locate required table {tableName} in source and/or destination database");
                    return true;
                }

            }

            return false;

        }

        /// <summary>
        /// Perform the synchronization.
        /// </summary>
        /// <returns>true, if the synchronization was successful; otherwise, false.</returns>
        public bool Sync()
        {
            Error = false;

            if (!Initialized) Init();

            for (int i = 0; i < Config.ReplicationSets.Count; i++)
            {
                var replicationSet = Config.ReplicationSets[i];
                var tables = Tables[i];

                if (!Sync(replicationSet, tables, replicationSet.ForceFullReload.GetValueOrDefault(false)))
                    Error = true;
            }

            Log.Info($"Finished replication {(Error ? "with" : "without")} errors");

            return !Error;
        }

        private bool Sync(ReplicationSet replicationSet, IList<TableInfo> tables, bool fullReload = false,  long sourceVersion = -1)
        {
            Error = false;

            if (!tables.Any()) return true;

            Log.Info($"Starting replication for replication set {replicationSet.Name}");


            Dictionary<string, List<DatabaseInfo>> byVersion = new();

            foreach( var d in replicationSet.Destinations)
            {

                var destTables = GetTables(d, false);

                if (SchemaChanged(destTables, tables, replicationSet.Tables ))
                {
                    return false;
                }

                var verData = GetCurrentVersion(d, tables.Select(x => x.Name).ToArray()).Select(kv => $"{kv.Key}:{kv.Value};").ToList();
                verData.Sort();
                var dstring = String.Join("", verData);

                if (!byVersion.ContainsKey(dstring))
                        byVersion.Add(dstring, new List<DatabaseInfo>());
                byVersion[dstring].Add(d);
            }

            foreach (var destSet in byVersion)
            {
                var versionData = GetCurrentVersion(destSet.Value[0], tables.Select(x => x.Name).ToArray());

                if (fullReload)
                {
                    foreach (var dest in destSet.Value)
                        FullLoadReplicate(replicationSet.Source, dest, tables);
                }
                else
                    Replicate(replicationSet.Source, destSet.Value, versionData, tables);


            }

            return !Error;
        }




        /// <summary>
        /// Performs synchronization in an infinite loop. Periodically checks if source version has increased to trigger replication.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        public void SyncLoop(CancellationToken token)
        {
            var currentVersions = Enumerable.Repeat(0L, Config.ReplicationSets.Count).ToList();

            if (!Initialized) Init();

            Dictionary<ReplicationSet, DateTime> delayUntil = new();

            while (true)
            {
                if (token.IsCancellationRequested)
                {
                    Log.Info("Stopping replication.");
                    return;
                }

                var start = DateTime.UtcNow;
                Error = false;

                for (int i = 0; i < Config.ReplicationSets.Count; i++)
                {
                    var replicationSet = Config.ReplicationSets[i];

                    if (delayUntil.TryGetValue(replicationSet, out DateTime dt) && dt > DateTime.UtcNow)
                        continue;

                    var defaultDelay = DateTime.UtcNow.AddSeconds(replicationSet.Interval ?? Interval);

                    var currentVersion = currentVersions[i];
                    long version = 0;

                    try
                    {
                        using (var db = GetDatabase(replicationSet.Source.ConnectionString, DatabaseType.SqlServer2008))
                        {
                            version = db.ExecuteScalar<long>("select CHANGE_TRACKING_CURRENT_VERSION()");

                            Log.Debug($"Current version of source in replication set {replicationSet.Name} is {version}.");

                            if (version > currentVersion)
                            {

                                var tables = Tables[i];
                                if (SchemaChanged(GetTablesEx(db), tables, replicationSet.Tables))
                                {
                                    throw new InvalidOperationException($"Schema change detected in {replicationSet.Name}");
                                }

                                Log.Info($"Current version of source in replication set {replicationSet.Name} has increased from {currentVersion} to {version}: Starting replication.");
                                var success = Sync(replicationSet, tables, false, version);

                                if (success) currentVersions[i] = version;

                                Synced?.Invoke(this, new SyncEventArgs { ReplicationSet = replicationSet, Version = version });
                            }
                        }
                    }
#pragma warning disable CA1031 // Do not catch general exception types
                    catch (Exception ex)
                    {
                        Log.Error(ex, $"Error occurred during replication of set {replicationSet.Name}.");
                        Error = true;
                    }
#pragma warning restore CA1031 // Do not catch general exception types

                    Log.Debug($"Replication set {replicationSet.Name} update is deferred until {defaultDelay}.");
                    delayUntil[replicationSet] = defaultDelay;

                    if (token.IsCancellationRequested)
                    {
                        Log.Info("Stopping replication.");
                        return;
                    }
                }


                if (Error)
                    Log.Info($"Finished replication {(Error ? "with" : "without")} errors");

                Thread.Sleep(1000);
            }
        }

        private IList<TableInfo> GetTablesEx(Database db, bool onlyTracked = true)
        {
            try
            {
                var trackWhere = onlyTracked ? @"and tr.object_id is not null" : "";

                var sql = @"select TableName, ColumnName, coalesce(max(cast(is_primary_key as tinyint)), 0) PrimaryKey,
                        coalesce(max(cast(is_identity as tinyint)), 0) IsIdentity,
						max(X.TypeDescription) as TypeDescription from
                        (
                        select ('[' + s.name + '].[' + t.name + ']') TableName, ('[' + COL_NAME(t.object_id, a.column_id) + ']') ColumnName,
                        i.is_primary_key, a.is_identity,

						cast(a.system_type_id as varchar(48)) +';'+
						cast(a.user_type_id as varchar(48)) +';'+
						cast(a.max_length as varchar(48)) +';'+
						cast(a.precision as varchar(48)) +';'+
						cast(a.scale as varchar(48)) +';'+
						cast(a.is_nullable as varchar(48)) +';'+
						cast(a.is_identity as varchar(48)) as TypeDescription

                        from sys.tables t 
                        join sys.schemas s on s.schema_id = t.schema_id
                        join sys.columns a on a.object_id = t.object_id
                        left join sys.index_columns c on c.object_id = t.object_id and c.column_id = a.column_id
                        left join sys.indexes i on i.object_id = t.object_id and i.index_id = c.index_id
						left join sys.change_tracking_tables tr on t.object_id = tr.object_id
                        where a.is_computed = 0
						" + trackWhere + @"
                        ) X
                        group by TableName, ColumnName
                        order by TableName, ColumnName";

                var tables = db.Fetch<dynamic>(sql).GroupBy(t => t.TableName)
                    .Select(g => new TableInfo
                    {
                        Name = (string)g.Key,
                        KeyColumns = g.Where(c => (int)c.PrimaryKey > 0).Select(c => (string)c.ColumnName).ToList(),
                        OtherColumns = g.Where(c => (int)c.PrimaryKey == 0).Select(c => (string)c.ColumnName).ToList(),
                        HasIdentity = g.Any(c => (int)c.IsIdentity > 0),
                        ColumnTypeDescriptions = g.Select( c => ( (string) c.ColumnName , (string)c.TypeDescription) ).ToDictionary( x=> x.Item1, x=> x.Item2 )
                    }).ToList();
                List<ForeignKeyConstraint> fks = GetAllEnabledConstraints(db);

                foreach (var table in tables)
                    table.ForeignKeyConstraints = fks.Where(f => f.TableName == table.Name).ToList();

                var uqcs = db.Fetch<UniqueColumn>(@"SELECT
                             IndexId = ('[' + sch.name + '].[' + t.name + '].[' + ind.name + ']'),
                             TableName = ('[' + sch.name + '].[' + t.name + ']'),
                             IndexName = ind.name,
                             ColumnName = ('[' +  col.name + ']'),
                             IsConstraint = is_unique_constraint
                        FROM
                             sys.indexes ind
                        INNER JOIN
                             sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id
                        INNER JOIN
                             sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
                        INNER JOIN
                             sys.tables t ON ind.object_id = t.object_id
                        inner join sys.schemas sch
                            on t.schema_id = sch.schema_id
                        WHERE
                             ind.is_primary_key = 0
                             AND (ind.is_unique = 1 or ind.is_unique_constraint = 1)
                             AND t.is_ms_shipped = 0
                        ORDER BY
                             t.name, ind.name, ind.index_id, ic.is_included_column, ic.key_ordinal;");

                var uqs = uqcs.GroupBy(c => $"{c.TableName}_{c.IndexName}").Select(g => new UniqueConstraint
                {
                    IndexName = g.First().IndexName,
                    TableName = g.First().TableName,
                    IsConstraint = g.First().IsConstraint,
                    ColumnNames = g.Select(c => c.ColumnName).ToList(),
                }).ToList();

                foreach (var table in tables)
                    table.UniqueConstraints = uqs.Where(u => u.TableName == table.Name).ToList();

                return tables;
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Error getting tables to replicate from source database");
                throw;
            }
        }

        private IList<TableInfo> GetTables(DatabaseInfo dbInfo, bool onlyTracked = true)
        {
            using (var db = GetDatabase(dbInfo.ConnectionString, DatabaseType.SqlServer2008))
            {
                return GetTablesEx(db, onlyTracked);
            }
        }

        private static List<ForeignKeyConstraint> GetAllConstraints(Database db)
        {

            return db.Fetch<ForeignKeyConstraint>(@"select obj.name AS ForeignKeyName,
                            ('[' + sch.name + '].[' + tab1.name + ']') TableName,
                            ('[' +  col1.name + ']') ColumnName,
                            ('[' + sch2.name + '].[' + tab2.name + ']') ReferencedTableName,
                            ('[' +  col2.name + ']') ReferencedColumnName,
                            obj.is_disabled as IsDisabled
                        from sys.foreign_key_columns fkc
                        inner join sys.foreign_keys obj
                            on obj.object_id = fkc.constraint_object_id
                        inner join sys.tables tab1
                            on tab1.object_id = fkc.parent_object_id
                        inner join sys.schemas sch
                            on tab1.schema_id = sch.schema_id
                        inner join sys.columns col1
                            on col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
                        inner join sys.tables tab2
                            on tab2.object_id = fkc.referenced_object_id
                        inner join sys.schemas sch2
                            on tab2.schema_id = sch2.schema_id
                        inner join sys.columns col2
                            on col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id"
                        );
        }

        private static List<ForeignKeyConstraint> GetAllEnabledConstraints(Database db)
        {
            return GetAllConstraints(db).Where(x => x.IsDisabled != 1).ToList();
        }

        private Database GetDatabase(string connectionString, DatabaseType databaseType = null)
        {
            var db = new Database(connectionString, databaseType ?? DatabaseType.SqlServer2005, System.Data.SqlClient.SqlClientFactory.Instance);

            if (Timeout != 0) db.CommandTimeout = Timeout;

            return db;
        }

        private ResultsBuffer AllocBufferForFull(TableInfo table)
        {
            List<string> cols = new();

            for (int i = 0; i < table.KeyColumns.Count; i++)
                cols.Add(table.KeyColumns[i]);
            for (int i = 0; i < table.OtherColumns.Count; i++)
                cols.Add(table.OtherColumns[i]);

            return new ResultsBuffer(cols.ToArray());
        }

        private async Task<bool> Save(Database db, ResultsBuffer buffer, TableInfo table)
        {
            if (DryRun) return
                        true;
            try
            {

                var insertSql = string.Format("insert into {0} ({1}) values ({2})", table.Name,
                                string.Join(", ", buffer.Columns),
                                string.Join(", ", Parameters(buffer.Columns.Length)));

                if (table.HasIdentity)
                    insertSql = $"set IDENTITY_INSERT {table.Name} ON; {insertSql}; set IDENTITY_INSERT {table.Name} OFF";



                int count = buffer.Count;

                while (buffer.Lines.Count > 0)
                {
                    object[] data = buffer.Lines.Dequeue();
                    Log.Debug($"Executing insert: {insertSql} ({FormatArgs(data)})");
                    await db.ExecuteAsync(insertSql, data);
                }

                Log.Info($"Saved {count} entries to {table.Name}");
                return true;
            } catch (Exception ex)
            {
                Log.Error(ex);
                return false;
            }

        }

        private void ToggleForgeignConstraints(Database db, IEnumerable<ForeignKeyConstraint> constraints, bool Enabled)
        {
            foreach (ForeignKeyConstraint constraint in constraints)
            {
                if (Enabled)
                    ReenableForeignKeyConstraint(db, constraint);
                else
                    DisableForeignKeyConstraint(db, constraint);
            }
        }

        private void FullLoadReplicate(DatabaseInfo source, DatabaseInfo destination, IList<TableInfo> tables)
        {

            using var dbDest = GetDatabase(destination.ConnectionString, DatabaseType.SqlServer2005);
            using var dbSrc = GetDatabase(source.ConnectionString, DatabaseType.SqlServer2008);

            var snapshotIsolationEnabled = dbSrc.ExecuteScalar<int>("select snapshot_isolation_state from sys.databases where name = DB_NAME()") == 1;
            if (!snapshotIsolationEnabled)
            {
                Log.Error($"Snapshot isolation is not enabled in database {source.Name}, need it to have it enabled");
                return;
            }


            using (var transSrc = dbSrc.GetTransaction(System.Data.IsolationLevel.Snapshot))
            {
                var currentVer = dbSrc.ExecuteScalar<long>("select CHANGE_TRACKING_CURRENT_VERSION()");
                Log.Info($"Current version of database {source.Name} is {currentVer}");

                VerifyChangeTrackingPresence(tables, dbSrc);

                var involvedTables = new HashSet<string>(tables.Select(t => t.Name));
                var destConstraints = GetAllConstraints(dbDest).ToList();
                var constraintsToDisable = GetAllEnabledConstraints(dbDest).Where(c => (involvedTables.Contains(c.TableName) || involvedTables.Contains(c.ReferencedTableName)) && c.IsDisabled != 1).ToList();

                ToggleForgeignConstraints(dbDest, constraintsToDisable, false);
                try
                {
                    TransferAllTables(tables, dbDest, dbSrc, destConstraints);
                    SetSyncVersion(dbDest, currentVer, tables.Select(x => x.Name).ToArray());
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Full re-load failed, you will likely need to re-start from scratch. Destination {destination.Name} state uncertain");
                    Error = true;
                }
                finally
                {
                    ToggleForgeignConstraints(dbDest, constraintsToDisable, true);
                }

            }
        }

        private void TransferAllTables(IList<TableInfo> tables, Database dbDest, Database dbSrc, List<ForeignKeyConstraint> destForeignKeyConstraints)
        {
            var tablesWithFK = new HashSet<string>(destForeignKeyConstraints.Select(x => x.TableName).Union(destForeignKeyConstraints.Select(x => x.ReferencedTableName) ) );

            foreach (TableInfo table in tables)
            {
                string sqlClean = tablesWithFK.Contains(table.Name) ? "delete from" : "truncate table";
                dbDest.Execute($"{sqlClean} {table.Name}");

                var sql = $@"select {string.Join(", ", table.KeyColumns.Select(c => "t." + c).Concat(table.OtherColumns.Select(c => "t." + c)))}
                            from {table.Name} t";

                dbSrc.OpenSharedConnection();
                var cmd = dbSrc.CreateCommand(dbSrc.Connection, System.Data.CommandType.Text, sql);

                using var reader = cmd.ExecuteReader();
                var numChanges = 0;

                ResultsBuffer buffer = null;
                Task<bool> saver = null;

                while (reader.Read())
                {
                    if (buffer == null)
                        buffer = AllocBufferForFull(table);
                    buffer.ReadLine(reader);
                    if (buffer.Count >= 1000)
                    {
                        WaitForSave(saver);
                        var saveBuffer = buffer;
                        buffer = null;
                        saver = Task.Run<bool>(() => Save(dbDest, saveBuffer, table));
                    }
                    numChanges++;
                }
                WaitForSave(saver);
                if (buffer != null)
                {
                    saver = Save(dbDest, buffer, table);
                    WaitForSave(saver);
                }
            }
        }

        private static void VerifyChangeTrackingPresence(IList<TableInfo> tables, Database dbSrc)
        {
            foreach (TableInfo table in tables)
            {
                var minVersion = dbSrc.ExecuteScalar<long?>("select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@0))", table.Name);
                if (!minVersion.HasValue)
                {
                    Log.Error($"Table {table.Name} does not have change tracking enabled");
                }
            }
        }

        private static void WaitForSave(Task<bool> saver)
        {
            if (saver == null)
                return;
            saver.Wait();
            if (!saver.Result)
                throw new Exception($"Saver failed {saver.Exception}");
        }

        private void Replicate(DatabaseInfo source, List<DatabaseInfo> destinations, Dictionary<string, long> versions, IList<TableInfo> tables)
        {
            var changeInfo = RetrieveChangesEx(source, versions, tables);
            if (changeInfo == null) return;

            // replicate changes to destinations
            foreach(var destination in destinations)
            {
                try
                {
                    Log.Info($"Replicating {"change".ToQuantity(changeInfo.Changes.Count)} to destination {destination.Name}");

                    using var db = GetDatabase(destination.ConnectionString, DatabaseType.SqlServer2005);
                    using (var transaction = db.GetTransaction(System.Data.IsolationLevel.ReadCommitted))
                    {

                        var existingDestConstraints = new HashSet<string>(GetAllConstraints(db).Select(e => e.FullName));

                        try
                        {
                            var changes = changeInfo.Changes;
                            var disabledForeignKeyConstraints = new HashSet<ForeignKeyConstraint>();


                            try
                            {
                                foreach (var change in changes)
                                {
                                    foreach (var key in change.Table.ForeignKeyConstraints)
                                    {
                                        if (!existingDestConstraints.Contains(key.FullName))
                                            continue;

                                        if (!disabledForeignKeyConstraints.Contains(key))
                                        {
                                            DisableForeignKeyConstraint(db, key);
                                            disabledForeignKeyConstraints.Add(key);
                                        }

                                    }
                                }


                                for (int i = 0; i < changes.Count; i++)
                                {
                                    var change = changes[i];
                                    Log.Debug($"Replicating change #{i + 1} of {changes.Count} (Version {change.Version}, CreationVersion {change.CreationVersion})");
                                    PerformChange(db, change);
                                }

                            }
                            finally
                            {
                                foreach (var fk in disabledForeignKeyConstraints.ToArray())
                                {
                                    if (!existingDestConstraints.Contains(fk.FullName))
                                        continue;

                                    ReenableForeignKeyConstraint(db, fk);
                                    disabledForeignKeyConstraints.Remove(fk);
                                }
                            }

                            if (!DryRun)
                            {
                                SetSyncVersion(db, changeInfo.Version, tables.Select(x => x.Name).ToArray());
                                transaction.Complete();
                            }

                            Log.Info($"Destination {destination.Name} now at version {changeInfo.Version}");
                        }
#pragma warning disable CA1031 // Do not catch general exception types
                        catch (Exception ex)
                        {
                            Error = true;
                            Log.Error(ex, $"Error replicating changes to destination {destination.Name}");
                        }
#pragma warning restore CA1031 // Do not catch general exception types
                    }
                }
#pragma warning disable CA1031 // Do not catch general exception types
                catch (Exception ex)
                {
                    Error = true;
                    Log.Error(ex, $"Error replicating changes to destination {destination.Name}");
                }
#pragma warning restore CA1031 // Do not catch general exception types

            }
        }

        private void ReenableForeignKeyConstraint(Database db, ForeignKeyConstraint fk)
        {
            Log.Debug($"Re-enabling foreign key constraint {fk.ForeignKeyName}");
            var sql = $"alter table {fk.TableName} with check check constraint [{fk.ForeignKeyName}]";
            if (!DryRun)
                db.Execute(sql);
        }

        private void DisableForeignKeyConstraint(Database db, ForeignKeyConstraint fk)
        {
            Log.Debug($"Disabling foreign key constraint {fk.ForeignKeyName}");
            var sql = $"alter table {fk.TableName} nocheck constraint [{fk.ForeignKeyName}]";
            if (!DryRun)
                db.Execute(sql);
        }

        private static string SerializeList(string[] items)
        {
            return JsonConvert.SerializeObject(items);
        }

        private void SetSyncVersion(Database db, long currentVersion, string[] tables )
        {
            if (!DryRun)
            {
                var syncInfoTableExists = db.ExecuteScalar<string>("select top(1) name from sys.tables where name ='SyncInfo'") != null;

                if (!syncInfoTableExists)
                {
                    db.Execute("create table SyncInfo (Id int not null primary key default 1 check (Id = 1), Version bigint not null, TablesList nvarchar(max))");
                    db.Execute("insert into SyncInfo (Version, TablesList) values (@0, @1)", currentVersion, SerializeList(tables));
                }
                else
                {
                    db.Execute("update SyncInfo set Version = @0, TablesList = @1", currentVersion, SerializeList(tables));
                }
            }
        }

        private ChangeInfo RetrieveChangesEx(DatabaseInfo source, Dictionary<string, long> versions, IList<TableInfo> tables)
        {

            var changeInfo = new ChangeInfo();
            var changes = new List<Change>();

            using (var db = GetDatabase(source.ConnectionString, DatabaseType.SqlServer2008))
            {
                var snapshotIsolationEnabled = db.ExecuteScalar<int>("select snapshot_isolation_state from sys.databases where name = DB_NAME()") == 1;
                if (snapshotIsolationEnabled)
                {
                    Log.Info($"Snapshot isolation is enabled in database {source.Name}");
                    db.BeginTransaction(System.Data.IsolationLevel.Snapshot);
                }
                else
                {
                    string messageIsolationRequired = $"Snapshot isolation is not enabled in database {source.Name}, need it to have it enabled";
                    Log.Error(messageIsolationRequired);
                    Error = true;
                    return null;
                }

                changeInfo.Version = db.ExecuteScalar<long>("select CHANGE_TRACKING_CURRENT_VERSION()");
                Log.Info($"Current version of database {source.Name} is {changeInfo.Version}");

                foreach (var table in tables)
                {
                    var tableName = table.Name;
                    var minVersion = db.ExecuteScalar<long?>("select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@0))", tableName);

                    Log.Info($"Minimum version of table {tableName} in database {source.Name} is {minVersion}");

                    var destinationVersion = versions[table.Name];
                    bool loadFull = (minVersion > destinationVersion);

                    if (loadFull)
                    {
                        Log.Warn($"Cannot replicate table {tableName} because minimum source version {minVersion} is greater than destination version {destinationVersion}. Attempting full load!");
                        loadFull = true;
                    }

                    string sql;

                    if (loadFull)
                    {
                        Log.Info($"Scheduling truncate in table {tableName}");
                        changes.Add(new Change { Operation = 'T', Table = table, Version = changeInfo.Version - 1 });
                        sql = $@"select {string.Join(", ", table.KeyColumns.Select(c => "t." + c).Concat(table.OtherColumns.Select(c => "t." + c)))}
                            from {table.Name} t";
                    } else
                    {
                        sql = $@"select c.SYS_CHANGE_OPERATION, c.SYS_CHANGE_VERSION, c.SYS_CHANGE_CREATION_VERSION,
                        {string.Join(", ", table.KeyColumns.Select(c => "c." + c).Concat(table.OtherColumns.Select(c => "t." + c)))}
                        from CHANGETABLE (CHANGES {tableName}, @0) c
                        left outer join {tableName} t on ";
                        sql += string.Join(" and ", table.KeyColumns.Select(k => $"c.{k} = t.{k}"));
                        sql += " order by coalesce(c.SYS_CHANGE_CREATION_VERSION, c.SYS_CHANGE_VERSION), c.SYS_CHANGE_OPERATION";
                    }

                    Log.Debug($"Retrieving changes for table {tableName}: {sql}");

                    db.OpenSharedConnection();
                    var cmd = db.CreateCommand(db.Connection, System.Data.CommandType.Text, sql, destinationVersion);

                    using var reader = cmd.ExecuteReader();
                    var numChanges = 0;
                    object[] nullableMap = null;

                    while (reader.Read())
                    {

                        if (nullableMap == null)
                            nullableMap = reader.GetNullableTypeMap();

                        var col = 0;

                        Change change = null;

                        if (!loadFull)
                        {
                            change = new Change { Operation = ((string)reader[col])[0], Table = table };
                            col++;
                            var version = reader.GetInt64(col);
                            change.Version = version;
                            col++;
                            var creationVersion = reader.IsDBNull(col) ? version : reader.GetInt64(col);
                            change.CreationVersion = creationVersion;
                            col++;
                        } else
                        {
                            change = new Change { Operation = 'I', Table = table, Version = changeInfo.Version };
                        }

                        for (int i = 0; i < table.KeyColumns.Count; i++, col++)
                        {
                            var val = reader.GetValue(col);
                            change.Keys[table.KeyColumns[i]] = (val == DBNull.Value ? (nullableMap[col] ?? val) : val);
                        }

                        for (int i = 0; i < table.OtherColumns.Count; i++, col++)
                        {
                            var val = reader.GetValue(col);
                            change.Others[table.OtherColumns[i]] = (val == DBNull.Value ? (nullableMap[col] ?? val) : val);

                        }

                        changes.Add(change);
                        numChanges++;
                    }

                    Log.Info($"Table {tableName} has {"change".ToQuantity(numChanges)}");
                }

                if (snapshotIsolationEnabled)
                    db.CompleteTransaction();
            }

            changeInfo.Changes.AddRange(changes.OrderBy(c => c.MergedVersion).ThenBy(c => c.Operation).ThenBy(c => c.Table.Name));
            ComputeForeignKeyConstraintsToDisable(changeInfo);
            return changeInfo;
        }


        private void ComputeForeignKeyConstraintsToDisable(ChangeInfo changeInfo)
        {
            changeInfo.InvolvedKeys.Clear();
            foreach(var change in changeInfo.Changes)
            {
                changeInfo.InvolvedKeys.AddRange(change.Table.ForeignKeyConstraints);
            }
        }

        private void PerformChange(Database db, Change change)
        {
            var table = change.Table;
            var tableName = table.Name;
            var operation = change.Operation;

            switch (operation)
            {
                // Insert
                case 'I':
                    var insertColumnNames = change.GetColumnNames();
                    var insertSql = string.Format("insert into {0} ({1}) values ({2})", tableName,
                        string.Join(", ", insertColumnNames),
                        string.Join(", ", Parameters(insertColumnNames.Count)));
                    var insertValues = change.GetValues();
                    if (table.HasIdentity)
                        insertSql = $"set IDENTITY_INSERT {tableName} ON; {insertSql}; set IDENTITY_INSERT {tableName} OFF";
                    Log.Debug($"Executing insert: {insertSql} ({FormatArgs(insertValues)})");
                    if (!DryRun)
                        db.Execute(insertSql, insertValues);
                    break;

                // Update
                case 'U':
                    var updateColumnNames = change.Others.Keys.ToList();
                    var updateSql = string.Format("update {0} set {1} where {2}", tableName,
                        string.Join(", ", updateColumnNames.Select((c, i) => $"{c} = @{i + change.Keys.Count}")),
                        PrimaryKeys(change));
                    var updateValues = change.GetValues();
                    Log.Debug($"Executing update: {updateSql} ({FormatArgs(updateValues)})");
                    if (!DryRun)
                        db.Execute(updateSql, updateValues);
                    break;

                // Delete
                case 'D':
                    var deleteSql = string.Format("delete from {0} where {1}", tableName, PrimaryKeys(change));
                    var deleteValues = change.Keys.Values.ToArray();
                    Log.Debug($"Executing delete: {deleteSql} ({FormatArgs(deleteValues)})");
                    if (!DryRun)
                        db.Execute(deleteSql, deleteValues);
                    break;
                // Truncate - actually artificial for full re-load
                case 'T':
                    var truncateSql = string.Format("truncate table {0}", tableName);
                    Log.Debug($"Executing truncate: {truncateSql}");
                    if (!DryRun)
                        db.Execute(truncateSql);

                    break;
            }
        }

        private static string FormatArgs(object[] args) => string.Join(", ", args.Select((a, i) => $"@{i} = {a}"));

        private static string PrimaryKeys(Change change) =>
            string.Join(" and ", change.Keys.Keys.Select((c, i) => $"{c} = @{i}"));

        private static IEnumerable<string> Parameters(int n) => Enumerable.Range(0, n).Select(c => "@" + c);

        private Dictionary<string, long> GetCurrentVersion(DatabaseInfo dbInfo, string[] TablesList)
        {
            var answer = new Dictionary<string, long>();

            foreach(var t in TablesList)
            {
                answer[t] = 0;
            }

            try
            {
                using var db = GetDatabase(dbInfo.ConnectionString, DatabaseType.SqlServer2005);
                var syncInfoTableExists = db.ExecuteScalar<string>("select top(1) name from sys.tables where name ='SyncInfo'") != null;
                long currentVersion;

                if (!syncInfoTableExists)
                {
                    Log.Info($"SyncInfo table does not exist in database {dbInfo.Name}, assuming version 0");
                }
                else
                {
                    db.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                    currentVersion = db.ExecuteScalar<long>("select top(1) Version from SyncInfo");
                    var tablesStored = db.ExecuteScalar<string>("select top(1) [TablesList] from SyncInfo");
                    db.CompleteTransaction();
                    var recordedTables = JsonConvert.DeserializeObject<string[]>(tablesStored);

                    foreach(var t in recordedTables)
                    {
                        answer[t] = currentVersion;
                    }
                }
                return answer;
            }
#pragma warning disable CA1031 // Do not catch general exception types
            catch (Exception ex)
            {
                Log.Error(ex, $"Error getting current version of destination database {dbInfo.Name}. Skipping this destination.");
                Error = true;
                return [];
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }
    }
}
