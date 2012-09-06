using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using DomainLib;

namespace MongoLab2
{
    [TestClass]
    public class UnitTest1
    {
        private MongoServer returnMongoServer()
        {
            var connectionString = "mongodb://localhost/?safe=true";
            return MongoServer.Create(connectionString);
        }
        [TestMethod]
        public void TestMethod1()
        {
            var server = returnMongoServer();

            //get DB
            var database = server.GetDatabase("iStore");
            var collection = database.GetCollection("people");//.GetCollection<People>("people");

            //Find all 
            var entity = collection.FindAllAs<People>();
            var iList = entity.ToList();

            foreach (var item in iList)
            {
                Console.WriteLine("data: {0}", item.age);
            }

            var p = new People();
            p.age = 10;
            p.name = "www";
            collection.Insert(p);



        }
    }
}
