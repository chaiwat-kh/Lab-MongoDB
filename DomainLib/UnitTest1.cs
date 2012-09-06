using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;

namespace DomainLib
{
    public class People
    {
        public ObjectId Id { get; set; }
        public string name { get; set; }
        public string sex { get; set; }
        public int age { get; set; }
    }

    public class Car
    {
        public ObjectId Id { get; set; }
        public string Code { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
        }
    }
}
