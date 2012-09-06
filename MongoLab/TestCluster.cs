using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;

namespace MongoLab
{
    [TestClass]
    public class TestCluster
    {      

        [TestMethod]
        public void GetDataFromCluster()
        {
            var server = MongoServer.Create("mongodb://PC_011");

            var db = server.GetDatabase("test");
            var posts = db.GetCollection("foo");

            Console.WriteLine("Total: {0}" , posts.Count());
        }

        [TestMethod]
        public void GetDataFromReplica()
        {
            var server = MongoServer.Create("mongodb://PC_011:10001,PC_011:10002,PC_011:10003");

            var db = server.GetDatabase("test");
            var posts = db.GetCollection("foo");
            var where = Query.EQ("age", 245774);

            var list = posts.Find(where).ToList();

            Console.WriteLine("Count: {0}", list.Count());

            //foreach (var item in list)
            //{
            //    Console.WriteLine("---- data: {0}", item[1]);
            //}


        }
        [TestMethod]
        public void GetDataFromSlave()
        {
            try
            {
                //mongodb://host1,host2,host3/?slaveOk=true
                var server = MongoServer.Create("mongodb://PC_011:10002/?slaveOk=true");
                var db = server.GetDatabase("test");
                var posts = db.GetCollection("foo");
                Console.WriteLine("Total: {0}", posts.Count());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        [TestMethod]
        public void InsertData()
        {
            try
            {
                var server = MongoServer.Create("mongodb://PC_011:10002");
                var db = server.GetDatabase("test");
                var foos = db.GetCollection("foo");
                Console.WriteLine("B4, Count: {0}", foos.Count());

                for (int i = 0; i < 10000; i++)
                {
                    foos.Insert(new BsonDocument { { "age", i } });
                }
                Console.WriteLine("Afer, Count: {0}", foos.Count());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

    }
}
