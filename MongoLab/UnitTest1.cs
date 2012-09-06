using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;
using DomainLib;

namespace MongoLab
{
    public class WebPageView
    {
        public ObjectId Id { get; set; }
        public string name { get; set; }
        public string sex { get; set; }
        public int age { get; set; }
    }

    [TestClass]
    public class UnitTest1
    {
        private MongoServer returnMongoServer()
        {
            var connectionString = "mongodb://localhost/?safe=true";
            return MongoServer.Create(connectionString);
        }
        [TestMethod]
        public void TestMongoDB()
        {
            var server = returnMongoServer();
            
            //get DB
            var database = server.GetDatabase("iStore");
            var collection = database.GetCollection<People>("people");

            //Find all 
            var entity = collection.FindAll();
            var iList = entity.ToList();

            Console.WriteLine("Total b4 insert: {0}", iList.Count());
            foreach (var item in iList)
            {
                Console.WriteLine("### Name: {0} | Sex: {1}", item.name, item.sex);
            }

            //Insert data
            var people = new People { name = "Miki" };
            collection.Insert(people);
            //Find new data
            var idInset = people.Id;
            var query = Query.EQ("_id", idInset);
            people = collection.FindOne(query);
            Console.WriteLine("");
            Console.WriteLine("Insert new data id: {0}", idInset);
            Console.WriteLine("      info. name: {0} | Sex: {1}", people.name, people.sex);

            //update name for new data
            Console.WriteLine("");
            Console.Write("Change name from: {0}", people.name);
            people.name = "Fire Man";
            people.sex = "Male";
            collection.Save(people);
            people = collection.FindOne(query);
            Console.WriteLine(" to: {0} | {1}", people.name, people.sex);

            //update again by fn update(_id)
            var update = Update.Set("name", "Lucus");
            collection.Update(query, update);
            people = collection.FindOne(query);
            Console.WriteLine("--------- update again");
            Console.WriteLine("              name: {0}", people.name);

            Console.WriteLine("");
            Console.WriteLine("Remove: {0}", people.name);
            collection.Remove(query);
            iList = collection.FindAll().ToList();
            Console.WriteLine("Total : {0}", iList.Count());            

        }

        [TestMethod]
        public void TestMongoDB_diff_field()
        {
            var server = returnMongoServer();
            var database = server.GetDatabase("iStore");
            var collection = database.GetCollection("people");
                        
            var iList = collection.FindAll().ToList();
            foreach (var item in iList)
            {
                
            }

        }

        [TestMethod]
        public void TestMongoDBLinq()
        {
            var server = returnMongoServer();
            var database = server.GetDatabase("iStore");
            var collection = database.GetCollection("people");

            var query = collection.AsQueryable<People>().Where(p => p.name.Count() == 4);
                        
            Console.WriteLine(query.Count());
            
        }

        [TestMethod]
        public void TestBson()
        {
            BsonDocument document = new BsonDocument();
            document.Add(new BsonElement("Name", "Booker"));
            
            int age = 0;
            document.Add("age", 19, checkValid(age));
            
            if (document.Contains("age") && document["age"].IsInt32)
            {
                age = document["age", 10].AsInt32;
            }

            BsonDocument nested = new BsonDocument {
                { "name", "John Doe" },
                { "address", new BsonDocument {
                    { "street", "123 Main St." },
                    { "city", "Centerville" },
                    { "state", "PA" },
                    { "zip", 12345}
                }}
            };
        }
        private bool checkValid(int age)
        {
            if (age > 0)
                return false;
            return true;
        }
        
        
    }
}
