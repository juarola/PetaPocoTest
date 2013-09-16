using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;

using PetaPoco;
using System.Data.Common;
using System.Data;

namespace PetaPocoTest
{
    /*
     * Plain Old C# Classes ("POCO") 
     * 
     */

    public class Container
    {
        public int ContainerId { get; set; }
        public string Modificator { get; set; }
        public DateTime ModificationDate { get; set; }
    }

    public class Bovine
    {
        public int BovineId { get; set; }
        public int EarNr { get; set; }
        public string NameShort { get; set; }

        public IdkarBovine IdkarBovine { get; set; }
    }

    public class IdkarBovine
    {
        public int HerdId { get; set; }

        public int EarNr { get; set; }
    }

    public class Customer
    {
        public long CustId { get; set; }
        public string CustName { get; set; }
    }

    public static class DbHelper
    {
        static Database _CurrentDb = null;
        public static Database CurrentDb()
        {
            if (_CurrentDb == null)
            {
                _CurrentDb = new DatabaseWithMVCMiniProfiler("MainConnectionString");
            }
            return _CurrentDb;
        }

        //public static Database CurrentDb()
        //{
        //    if (HttpContext.Current.Items["CurrentDb"] == null)
        //    {
        //        var retval = new DatabaseWithMVCMiniProfiler("MainConnectionString");
        //        HttpContext.Current.Items["CurrentDb"] = retval;
        //        return retval;
        //    }
        //    return (Database)HttpContext.Current.Items["CurrentDb"];
        //}
    }

    public class DatabaseWithMVCMiniProfiler : PetaPoco.Database
    {
        public DatabaseWithMVCMiniProfiler(System.Data.IDbConnection connection) : base(connection) { }
        public DatabaseWithMVCMiniProfiler(string connectionStringName) : base(connectionStringName) { }
        public DatabaseWithMVCMiniProfiler(string connectionString, string providerName) : base(connectionString, providerName) { }
        public DatabaseWithMVCMiniProfiler(string connectionString, DbProviderFactory dbProviderFactory) : base(connectionString, dbProviderFactory) { }

        public override IDbConnection OnConnectionOpened(IDbConnection connection)
        {
            // wrap the connection with a profiling connection that tracks timings 
            return MvcMiniProfiler.Data.ProfiledDbConnection.Get(connection as DbConnection, MiniProfiler.Current);
        }
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // PetaPoco db-objektin luominen, tämän läpi tehdään kaikki jännä
                var db = new PetaPoco.Database("hep"); // connection stringin nimi parameetterinä

                BasicQuery(db);

                TransactionTest(db);

                UspQuery(db);

                var herd = 1505147;
                DateTime? date = DateTime.Now;

                DynamicSql(db, herd, date);

                var newContainer = new Container { ContainerId = 59004, Modificator = "JukkaA", ModificationDate = DateTime.Now };

                TestMARS(db);
                TestMapping(db);

                TestCreateOperation(db, newContainer);
                TestDeleteOperation(db, newContainer);
                
                TestPaging(db, herd, date);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            Console.ReadKey();
        }

        private static void TestMARS(Database db)
        {
            Console.WriteLine("mapping");
            Console.WriteLine();

            var herdId = 1505147;

            var sql =
                Sql.Builder.Append("execute hepsis");

            var res = db.Query<IdkarBovine>(sql);
        }

        private static void TestMapping(Database db)
        {
            Console.WriteLine("mapping");
            Console.WriteLine();

            var herdId = 1505147;

            var sql =
                Sql.Builder.Append("select * from bovine b with(nolock)")
                   .Append("join idkarbovine ib with(nolock) on b.BovineId = ib.BovineId")
                   .Append("where ib.Idkar=@0", herdId);

            var res = db.Query<Bovine, IdkarBovine, Bovine>(
                                                            (p, a) =>
                                                            {
                                                                p.IdkarBovine = a;
                                                                return p;
                                                            }, sql);
        }

        private static void TestPaging(Database db, int herd, DateTime? date)
        {
            Console.WriteLine("sivutettu haku dynaamisella sql:llä...");
            Console.WriteLine();

            var sql2 = PetaPoco.Sql.Builder
                .Append("SELECT * FROM idkarbovine with(nolock)")
                .Append("WHERE idkar=@0", herd)
                .Append("AND ValidFromDate<=@0", date.Value)
                .Append("AND ValidDueDate>=@0", date.Value)
                .Append("ORDER BY EarNr");

            var page1 = db.Page<Bovine>(1, 10, sql2);
            Console.WriteLine("Total items:" + page1.TotalItems);
            Console.WriteLine("Total pages:" + page1.TotalPages);
            Console.WriteLine("Current page:" + page1.CurrentPage);
            foreach (var a in page1.Items)
            {
                Console.WriteLine("{0} - {1}, {2}", a.BovineId, a.EarNr, a.NameShort);
            }

            Console.WriteLine();
            var page2 = db.Page<Bovine>(2, 10, sql2);
            Console.WriteLine("Total items:" + page2.TotalItems);
            Console.WriteLine("Total pages:" + page2.TotalPages);
            Console.WriteLine("Current page:" + page2.CurrentPage);
            foreach (var a in page2.Items)
            {
                Console.WriteLine("{0} - {1}, {2}", a.BovineId, a.EarNr, a.NameShort);
            }
            Console.WriteLine();

            var page3 = db.Page<Bovine>(3, 10, sql2);
            Console.WriteLine("Total items:" + page3.TotalItems);
            Console.WriteLine("Total pages:" + page3.TotalPages);
            Console.WriteLine("Current page:" + page3.CurrentPage);
            foreach (var a in page3.Items)
            {
                Console.WriteLine("{0} - {1}, {2}", a.BovineId, a.EarNr, a.NameShort);
            }

            Console.WriteLine();
            var page4 = db.Page<Bovine>(4, 10, sql2);
            Console.WriteLine("Total items:" + page4.TotalItems);
            Console.WriteLine("Total pages:" + page4.TotalPages);
            Console.WriteLine("Current page:" + page4.CurrentPage);
            foreach (var a in page4.Items)
            {
                Console.WriteLine("{0} - {1}, {2}", a.BovineId, a.EarNr, a.NameShort);
            }
        }

        private static void TestDeleteOperation(Database db, Container newContainer)
        {
            Console.WriteLine("poistetaan: {0} - {1}, {2}", newContainer.ContainerId, newContainer.Modificator, newContainer.ModificationDate);
            db.Delete("Container", "ContainerId", newContainer);
            Console.WriteLine("poistettu: {0} - {1}, {2}", newContainer.ContainerId, newContainer.Modificator, newContainer.ModificationDate);
        }

        private static void TestCreateOperation(Database db, Container newContainer)
        {
            // CREATE

            Console.WriteLine("tallennetaan: {0} - {1}, {2}", newContainer.ContainerId, newContainer.Modificator, newContainer.ModificationDate);

            db.Insert("Container", "ContainerId", false, newContainer);


            Console.WriteLine("tallennettu: {0} - {1}, {2}", newContainer.ContainerId, newContainer.Modificator, newContainer.ModificationDate);
            Console.WriteLine();
        }

        private static void DynamicSql(Database db, int herd, DateTime? date)
        {
            // Kysely dynaamisella sql:llä

            var sql = PetaPoco.Sql.Builder
                //.Append("SELECT * FROM idkarbovine with(nolock) sadfdsf") // Eri näppärä virhekäsittely, breakpoint petapoco.cs:n OnException-metodiin.
                .Append("SELECT * FROM idkarbovine with(nolock)")
                .Append("WHERE idkar=@0", herd);

            if (date.HasValue)
                sql.Append("AND ValidFromDate<=@0", date.Value);

            if (date.HasValue)
                sql.Append("AND ValidDueDate>=@0", date.Value);

            sql.Append("ORDER BY EarNr");

            var results = db.Query<Bovine>(sql);

            foreach (var a in results)
            {
                Console.WriteLine("{0} - {1}, {2}", a.BovineId, a.EarNr, a.NameShort);
            }
            Console.WriteLine();
        }

        private static void UspQuery(Database db)
        {
            // Kysely uspilla
            var customerId = 287350;
            var viaUsp = db.SingleOrDefault<Customer>("execute [usp_MLYLdaGetCustomer] @0", customerId);
            Console.WriteLine("{0} - {1}", viaUsp.CustId, viaUsp.CustName);
            Console.WriteLine();
        }

        private static void TransactionTest(Database db)
        {
            // Transaktio
            using (var tx = new TransactionScope())
            {
                // Lähderivi
                var source = db.SingleOrDefault<Customer>("SELECT * FROM Customer WITH(NOLOCK) WHERE CustId = 287350");

                // Kohderivi
                var target = db.SingleOrDefault<Customer>("SELECT * FROM Customer WITH(NOLOCK) WHERE CustId = 287348");

                target.CustName = source.CustName;

                // Vars. muutos: "taulu", "avain", data
                db.Update("customer", "custid", target);

                // Luetaan testimielessä likainen rivi
                var dirty = db.SingleOrDefault<Customer>("SELECT * FROM Customer WITH(NOLOCK)  WHERE CustId = 287348");
                Console.WriteLine("dirty: {0} - {1}", dirty.CustId, dirty.CustName);
                Console.WriteLine();
            }

            // Ei committia, eli ei tallentunut
            var rolledback = db.SingleOrDefault<Customer>("SELECT * FROM Customer WITH(NOLOCK)  WHERE CustId = 287348");
            Console.WriteLine("eiku: {0} - {1}", rolledback.CustId, rolledback.CustName);
            Console.WriteLine();
        }

        private static void BasicQuery(Database db)
        {
            // Peruskysely
            var customers = db.Query<Customer>("SELECT TOP 10 * FROM Customer WITH(NOLOCK)");

            foreach (var a in customers)
            {
                Console.WriteLine("{0} - {1}", a.CustId, a.CustName);
            }
            Console.WriteLine();
        }
    }
}
