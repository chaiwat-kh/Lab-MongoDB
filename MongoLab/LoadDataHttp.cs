using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Driver;
using System.Net;
using MongoDB.Bson;

namespace MongoLab
{
    [TestClass]
    public class LoadDataHttp
    {
        private MongoServer returnMongoServer()
        {
            //var connectionString = "mongodb://localhost/?safe=true";
            var connectionString = "mongodb://localhost:27017";
            return MongoServer.Create(connectionString);
        }

        private string getHtml(string httpWeb)
        {
            System.Net.WebRequest req = System.Net.WebRequest.Create(httpWeb);
            req.Proxy = new System.Net.WebProxy("192.168.1.220:80", true);
            req.Proxy.Credentials = CredentialCache.DefaultCredentials;

            string htmlContend = "";
            using (var reqStrem = req.GetResponse())
            {
                System.IO.TextReader reader = new System.IO.StreamReader(reqStrem.GetResponseStream());
                htmlContend = reader.ReadToEnd();
            }

            return htmlContend;
        }

        [TestMethod]
        public void TestMethod1()
        {
            var httpArr = new List<string>();
            httpArr.Add("http://www1.skysports.com/football/");

            var server = returnMongoServer();
            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");
            int index = 0;
            foreach (var item in httpArr)
            {
                if (!item.Equals(""))
                {
                    var html = getHtml(item);
                    int start = html.IndexOf("<div class=\"v5-main-news \">") + 27;
                    int end = html.IndexOf("</div><!--v5-main-news-->") - start;
                    if (start > 27)
                    {
                        html = html.Substring(start, end);

                        var sp1 = html.Split(new string[] { "<div class=\"v5-box \">" }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (var res in sp1)
                        {
                            char[] delimiters = new char[] { '\t', '\n' };
                            string[] parts = res.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);

                            if (parts.Length > 3)
                            {
                                string href = "";
                                string img = "";
                                string headText = "";
                                string detailText = "";

                                int lstart = parts[0].IndexOf("<a href=\"") + 9;
                                int lend = parts[0].IndexOf("\">");
                                int lcount = lend - lstart;

                                if (lstart >= 9)
                                {
                                    href = parts[0].Substring(lstart, lcount);

                                    string strImage = parts[0].Substring(lend + 2);
                                    lstart = 0;
                                    lend = 0;
                                    lcount = 0;
                                    
                                    lstart = strImage.IndexOf("<img src=\"") + 10;
                                    if (lstart >= 10)
                                    {
                                        strImage = strImage.Substring(lstart);

                                        lend = strImage.IndexOf("\"");
                                        lcount = lend;


                                        img = strImage.Substring(0, lcount);

                                        lstart = 0;
                                        lend = 0;
                                        lcount = 0;

                                        lstart = strImage.IndexOf("alt=\"") + 5;
                                        if (lstart >= 5)
                                        {
                                            string strHead = strImage.Substring(lstart);
                                            lcount = strHead.IndexOf("\"");
                                            headText = strHead.Substring(0, lcount);
                                        }
                                    }                                    
                                }

                                lstart = 0;
                                lend = 0;
                                lcount = 0;
                                                                
                                lstart = parts[2].IndexOf("<p>") + 3;
                                lend = parts[2].IndexOf("</p>");
                                lcount = lend - lstart;

                                if (lstart >= 3)
                                {
                                    detailText = parts[2].Substring(lstart, lcount);
                                }
                                
                                BsonDocument bsonDoc = new BsonDocument{
                                    { "Id", ++index},
                                    { "Url", item},
                                    { "Href", href},
                                    { "Img", img},
                                    { "HeadText", headText},
                                    { "DetailText", detailText},
                                };
                                collection.Insert(bsonDoc);
                            }

                        }                        
                    }
                }
            }
        }
        [TestMethod]
        public void GetData()
        {
            var server = returnMongoServer();
            var database = server.GetDatabase("testdb");
            var collection = database.GetCollection("example");

            var iList = collection.FindAll().ToList();
            foreach (var item in iList)
            {
                Console.WriteLine("{0}-{1}-{2}-{3}",item[1],item[2],item[3],item[4]);
            }

        }
        [TestMethod]
        public void splitString()
        {
            string data = "<THE > xx QUICKxxBROWNxxFOX<img src=";
            var result = data.Split(new string[] { "<div class=\"v5-box \">" }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var item in result)
            {
                Console.WriteLine(item.ToString());
            }
        }
    }
}
