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
using System.Net;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

namespace MongoLab
{
    [TestClass]
    public class TestShare
    {
        private MongoServer returnMongoServer()
        {
            //var connectionString = "mongodb://localhost/?safe=true";
            var connectionString = "mongodb://localhost:28021";
            return MongoServer.Create(connectionString);
        }

        [TestMethod]
        public void TestInsertData()
        {
            var server = MongoServer.Create("mongodb://192.168.1.176:27017");
            //get DB
            var database = server.GetDatabase("dbtest");
            var collection = database.GetCollection("foo");

            int index = 100000;
            for (int i = 1; i < index; i++)
            {
                BsonDocument bs = new BsonDocument
                {
                    {"number", i + 100000},
                    {"Name", "Lennon" + (i + 100000).ToString()}
                };
                collection.Insert(bs);
            }

            var iList = collection.FindAll().ToList();
            var count = iList.Count();
            Console.WriteLine("Count: {0}", count);

            
        }
        [TestMethod]
        public void TestShardFindData()
        {
            var server23 = MongoServer.Create("mongodb://192.168.1.176:21023");
            var server24 = MongoServer.Create("mongodb://192.168.1.176:21024");

            var database23 = server23.GetDatabase("foo");
            var collection23 = database23.GetCollection("bar");

            var serList23 = collection23.AsQueryable().Take(10).ToList();
            serList23.ForEach(i =>
                {
                    Console.WriteLine("Hash: " + i.AsBsonDocument["hash"].ToString());
                });

            var count = collection23.AsQueryable().Count();
            Console.WriteLine("Server 23 count: {0}", count);

            var database24 = server24.GetDatabase("foo");
            var collection24 = database24.GetCollection("bar");

            count = collection24.AsQueryable().Count();
            Console.WriteLine("Server 24 count: {0}", count);   
        }

        [TestMethod]
        public void TestFindData()
        {
            var server23 = MongoServer.Create("mongodb://192.168.1.176:21023");
            var server24 = MongoServer.Create("mongodb://192.168.1.176:21024");
            
            var database23 = server23.GetDatabase("foo");
            var collection23 = database23.GetCollection("bar");

            var result = collection23.AsQueryable().FirstOrDefault();
            Console.WriteLine("Server 23");
            if (result != null)
            {
                Console.WriteLine("### info first: {0}-{1}", result["ikey"].ToString(), result["Name"].ToString());                
            }

            result = collection23.AsQueryable().LastOrDefault();
            if (result != null)
            {
                Console.WriteLine("### info last: {0}-{1}", result["ikey"].ToString(), result["Name"].ToString());
            }

            var database24 = server24.GetDatabase("testdb");
            var collection24 = database24.GetCollection("employee");

            var result24 = collection24.AsQueryable().FirstOrDefault();
            Console.WriteLine("Server 24");

            if (result24 != null)
            {                
                Console.WriteLine("### info first: {0}-{1}", result["ikey"].ToString(), result["Name"].ToString());  
            }
            result = collection24.AsQueryable().LastOrDefault();
            if (result != null)
            {
                Console.WriteLine("### info last: {0}-{1}", result["ikey"].ToString(), result["Name"].ToString());
            }

        }

        [TestMethod]
        public void TestCountData()
        {
            var server23 = MongoServer.Create("mongodb://localhost:28023");
            var server24 = MongoServer.Create("mongodb://localhost:28024");

            var database23 = server23.GetDatabase("testdb");
            var collection23 = database23.GetCollection("things");

            var count = collection23.AsQueryable().Count();
            Console.WriteLine("Server 23 count: {0}", count);

            var database24 = server24.GetDatabase("testdb");
            var collection24 = database24.GetCollection("things");

            count = collection24.AsQueryable().Count();
            Console.WriteLine("Server 24 count: {0}", count);                    

        }

        [TestMethod]
        public void TestControl()
        {
            var server23 = MongoServer.Create("mongodb://localhost:28021");

            var database23 = server23.GetDatabase("testdb");
            var collection23 = database23.GetCollection("employee");

            var count = collection23.AsQueryable().Count();
            Console.WriteLine("Server 21 count: {0}", count);

        }
        public class Employee
        {
            public ObjectId Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [TestMethod]
        public void TestInsertData2()
        {
            var server = returnMongoServer();
            //get DB
            var database = server.GetDatabase("testdb");
            var collection = database.GetCollection("things");

            int index = 20000;
            for (int i = 0; i < index; i++)
            {
                BsonArray array = new BsonArray { "Tom", "Boon" };
                BsonDocument bs = new BsonDocument
                {
                    {"ikey", i},
                    {"Name", array}
                };
                collection.Insert(bs);
            }

            var iList = collection.FindAll().ToList();
            var count = iList.Count();
            Console.WriteLine("Count: {0}", count);


        }

        [TestMethod]
        public void LoadAndSaveToMongoHttp()
        {
            var httpArr = new string[5];
            httpArr[0] = "http://www.eastoftheweb.com/short-stories/UBooks/KingGris.shtml";
            httpArr[0] = "http://www.telegraph.co.uk/culture/film/8223897/The-Kings-Speech-the-real-story.html";
            httpArr[2] = "http://www.theatrehistory.com/american/musical011.html";
            httpArr[3] = "http://womenshistory.about.com/od/leonowensanna/a/anna_king_true.htm";
            httpArr[4] = "http://www.hipark.austin.isd.tenet.edu/mythology/midas.html";

            var server = returnMongoServer();
            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");
            int i = 0;
            foreach (var item in httpArr)
	        {
                if (!item.Equals(""))
                {
                    var html = getHtml(item);
                    int start = html.IndexOf("<body>");
                    int end = html.IndexOf("</body>") - start;
                    if(start > 0){

                        html = html.Substring(start, end);

                        BsonDocument bsonDoc = new BsonDocument{
                            { "Url", item},
                            { "Html", html}
                        };
                        collection.Insert(bsonDoc);
                    }
                }
            }
        }
        [TestMethod]
        public void InsertILoveYouToSimpleExmaple()
        {
            var server = returnMongoServer();
            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");

            collection.Insert(new BsonDocument{
                        { "Url", "1"},
                        { "Html", "I love you. I love a dog"},
                        { "detail", "suckseed"}
                    });
            collection.Insert(new BsonDocument{
                        { "Url", "2"},
                        { "Html", "I love you."},
                        { "detail", "metro"}
                    });
        }
        private BsonArray getBsonFromHtml(string httpWeb)
        {
            BsonArray bArr = new BsonArray();
            System.Net.WebRequest req = System.Net.WebRequest.Create(httpWeb);
            req.Proxy = new System.Net.WebProxy("192.168.1.219:80", true);
            req.Proxy.Credentials = CredentialCache.DefaultCredentials;
            
            using (var reqStrem = req.GetResponse())
            {
                System.IO.TextReader reader = new System.IO.StreamReader(reqStrem.GetResponseStream());
                var htmlContend = reader.ReadToEnd();

                char[] separator = new char[] { ' ', '\r', '\n', '\t' };
                var strArr = htmlContend.Split(separator, StringSplitOptions.RemoveEmptyEntries);
                if (strArr != null)
                {
                    bArr = new BsonArray(strArr);
                }                
            }
            return bArr;
        }
        private string getHtml(string httpWeb)
        {
            System.Net.WebRequest req = System.Net.WebRequest.Create(httpWeb);
            req.Proxy = new System.Net.WebProxy("192.168.1.219:80", true);
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
        public void TestCreateMapReduceFunction()
        {
            var server = returnMongoServer();

            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("things");

            //var Html = 'I love you. I love a dog';

            var map = new BsonJavaScript(@"
                            function() {
                                var HtmlArr = this.Html.split(' ');   
                                var list = [];    
                                var cc = HtmlArr.length;                        
                                for (var i = 0; i < HtmlArr.length; i++)
                                {
                                    var r = { vocab: "", count : 0 };
                                    
				                    if(list.length > 0)
			                        {

					                    var chkIn = 0;
                                        for (var j = 0; j < list.length; j++)
                                        {
                                            if(HtmlArr[i] == list[j]['vocab'])
                                            {			
						                        chkIn = 1;
						                        list[j]['count'] = list[j]['count'] + 1;
						                        break;
                                            } 				                                                  
                                        }
					                    if(chkIn == 0)
					                    {
                                            t._id = i;               
						                    r.vocab = HtmlArr[i];
                                            r.count = 1;
                                            list.push(r);                                    		
			                            } 
				                    }
				                    else
				                    {   
                                        t._id = i;                                 
					                    r.vocab = HtmlArr[i];
                                        r.count = 1;
                                        list.push(r);                                   
			                        }
                                }             
                                
                                list.forEach(
                                    function(z){
                                        emit(z.vocab, { count: z.count });
                                        }
                                    );                                
                                }");

            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                var sum = 0;
                                for ( var i=0; i<values.length; i++ ) 
                                    sum += values[i].count;
                                return {totalVocab: sum};
                            }");
            
            var mapRet = collection.MapReduce(map, reduce);
            var ret = mapRet.GetResults();

            for (int i = 0; i < ret.Count(); i++)
            {                
                collection.Insert(ret.ElementAt(i));
            }

        }

        [TestMethod]
        public void SimpleExample()
        {
            var server = returnMongoServer();

            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");

            var map = new BsonJavaScript(@"
                            function() {  
                                var clearEmtry = function(html){
                                    var strHTML = html.replace(/<\/?[a-z][a-z0-9]*[^<>]*>/ig, "");
                                    var strArr = strHTML.split(' ');                                     
                                    return strArr;
                                } 
   
                                var strArr = clearEmtry(this.Html);                      
                                strArr.forEach(
                                    function(z){
                                        emit(z, { totalVocab: 1, vocab: z });
                                        }
                                    );                                
                                }");

            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                var sum = 0;
                                var vocab = '';
                                for ( var i=0; i<values.length; i++ ) {
                                    sum += values[i].totalVocab;
                                }
                                return {totalVocab: sum, vocab: key};
                            }");

            var mapRet = collection.MapReduce(map, reduce);
            var ret = mapRet.GetResults().Take(20).ToList();

            ret.ForEach(r =>
            {
                var val = r["value"].AsBsonDocument;

                Console.WriteLine("vocab: {0}, totalVocab: {1}", val["vocab"].AsString, val["totalVocab"]);
            });
        }

        [TestMethod]
        public void SimpleExampleAddFunction()
        {
            var server = returnMongoServer();

            var db = server.GetDatabase("tstdb");
            var collection = db.GetCollection("things");

            var map = new BsonJavaScript(@"
                            function() {
                                   
                                var strArr = this.Html.split(' '); 
                                var list = [];

                                for (var i = 0; i < strArr.length; i++)
                                {        
                                    var r = { vocab: '', count: 1};
                                    r.vocab = strArr[i];
                                    r.count = 10;                                                                
                                    list.push(r);
                                }
                                    
                                list.forEach(
                                    function(z){
                                        emit(z.vocab, { totalVocab: z.count, vocab: z.vocab });
                                        }
                                    );                                
                                }");

            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                var sum = 0;
                                
                                for ( var i=0; i<values.length; i++ ) {
                                    sum += values[i].totalVocab;                                    
                                    
                                }
                                
                                return {totalVocab: sum, vocab: key};
                            }");

            var mapRet = collection.MapReduce(map, reduce);

            var ret = mapRet.GetResults().Take(20).ToList();
            
            ret.ForEach(r =>
            {
                var val = r["value"].AsBsonDocument;

                Console.WriteLine("vocab: {0}, count: {1}", val["vocab"].AsString, val["totalVocab"]);
            });
        }

        [TestMethod]
        public void SimpleExampleRemoveHTMLjvsFunc()
        {
            var server = returnMongoServer();

            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");

            var map = new BsonJavaScript(@"
                            function() {  
                                var clearEmtry = function(html){;
                                    var strHTML = html.replace(/<\/?[a-z][a-z0-9]*[^<>]*>/ig, '');            
                                    var strArr = strHTML.split(' ');                                     
                                    return strArr;
                                } 
   
                                var strArr = clearEmtry(this.Html);                      
                                strArr.forEach(
                                    function(z){
                                        emit(z, { totalVocab: 1, vocab: z });
                                        }
                                    );                                
                                }");

            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                var sum = 0;
                                var vocab = '';
                                for ( var i=0; i<values.length; i++ ) {
                                    sum += values[i].totalVocab;
                                }
                                return {totalVocab: sum, vocab: key};
                            }");

            var mapRet = collection.MapReduce(map, reduce);
            var ret = mapRet.GetResults().Take(100).ToList();

            ret.ForEach(r =>
            {
                var val = r["value"].AsBsonDocument;

                Console.WriteLine("vocab: {0}, totalVocab: {1}", val["vocab"].AsString, val["totalVocab"]);
            });
        }

        [TestMethod]
        public void LoadClearHtml()
        {
            var httpArr = new string[]{
               // "http://www.telegraph.co.uk/culture/film/8223897/The-Kings-Speech-the-real-story.html",
               // "http://www.fcbarcelona.com/football/detail/card/history-of-fc-barcelona",
                "http://www.realmadrid.com/cs/Satellite/en/1193041516534/Historia/Club.htm"
            };

            var server = returnMongoServer();
            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");

            foreach (var item in httpArr)
            {
                if (!item.Equals(""))
                {
                    var html = getHtml(item);
                    int start = html.IndexOf("<body");
                    int end = html.IndexOf("</body>") - start;
                    if (start > 0)
                    {
                        html = html.Substring(start, end);
                        var htmlStrip = HtmlStrip(html);
                        htmlStrip = htmlStrip.ToLower();                                             

                        BsonDocument bsonDoc = new BsonDocument{
                            { "Url", item},
                            { "Text", htmlStrip}
                        };
                        collection.Insert(bsonDoc);
                    }
                }
            }
        }
        public static string HtmlStrip(string input)
        {
            input = Regex.Replace(input, "<style>(.|\n|)*?</style>", " ");
            input = Regex.Replace(input, "\n|\t|\r|&nbsp;|&amp;|&quot;|&lt;|&gt;|&rsquo;|&ndash;|&raquo;|,", " ");
            input = Regex.Replace(input, @"<xml>(.|\n)*?</xml>", " "); // remove all <xml></xml> tags and anything inbetween.                          
            input = Regex.Replace(input, @"(?></?\w+)(?>(?:[^>'""]+|'[^']*'|""[^""]*"")*)>", " ");
            input = Regex.Replace(input, @"<(.|\n)*?>", " "); // remove any tags but not there content "<p>bob<span> johnson</span></p>" becomes "bob johnson"
            
            return Regex.Replace(input, "\\s+", " ");

             
        }

        [TestMethod]
        public void SimpleExampleClearHtml()
        {
            var server = returnMongoServer();

            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");

            var map = new BsonJavaScript(@"
                            function() {  
                                var clearHtml = function(text){
                                    
                                    var strArr = text.split(/[\s,]+/);                                    
                                    return strArr;
                                } 
   
                                var strArr = clearHtml(this.Text);                      
                                strArr.forEach(
                                    function(z){
                                        emit(z, { totalVocab: 1, vocab: z });
                                        }
                                    );                                
                                }");

            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                var sum = 0;
                                var vocab = '';
                                for ( var i=0; i<values.length; i++ ) {
                                    sum += values[i].totalVocab;
                                }
                                return {totalVocab: sum, vocab: key};
                            }");

            var mapRet = collection.MapReduce(map, reduce);
            var ret = mapRet.GetResults().ToList();

            ret.ForEach(r =>
            {
                var val = r["value"].AsBsonDocument;

                Console.WriteLine("Vocab: {0}, ### total: {1}", val["vocab"].AsString, val["totalVocab"]);
            });
        }

        [TestMethod]
        public void TestFindMapReduceResult()
        {
            var server = returnMongoServer();

            var db = server.GetDatabase("testdb");
            var collection = db.GetCollection("example");

            var output = collection.AsQueryable().ToList();

            foreach (var item in output)
            {
                Console.WriteLine("Url: {0}, Text: {1}", item["Url"], item["Text"]);
            }
        }

        [TestMethod]
        public void TestGetLogsDcm()
        {
            var server17 = MongoServer.Create("mongodb://localhost:27017");

            var database = server17.GetDatabase("logs");
            var collection = database.GetCollection("logs_dcm");

            var result = collection.AsQueryable().ToList();

            foreach (var item in result)
	        {
                Console.WriteLine("Value: {0}", item.ToString());
                Console.WriteLine("Json: {0}", JsonConvert.DeserializeObject(item["message"].AsString));
                Console.WriteLine("----------------");
	        }
        }

        [TestMethod]
        public void TestGetMasterNslave()
        {
            try
            {
                //controller
                var server17 = MongoServer.Create("mongodb://websit02:27017");
                var database17 = server17.GetDatabase("foo");
                var collection17 = database17.GetCollection("bar");
                var count = collection17.AsQueryable().Count();
                Console.WriteLine("27017 count: {0}", count);

                //master
                var svrMaster = MongoServer.Create("mongodb://websit02:27023");
                var dbMaster = svrMaster.GetDatabase("foo");
                var collMaster = dbMaster.GetCollection("bar");
                count = collMaster.AsQueryable().Count();
                Console.WriteLine("Master count: {0}", count);

                //slave
                var svrSlave = MongoServer.Create("mongodb://websit02:27024");
                var dbSlave = svrSlave.GetDatabase("foo");
                var collSlave = dbSlave.GetCollection("bar");
                count = collSlave.AsQueryable().Count();
                Console.WriteLine("Slave count: {0}", count);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        [TestMethod]
        public void TestInsertDataMasterNslave()
        {
            var server = MongoServer.Create("mongodb://websit02:27017");
            //get DB
            var database = server.GetDatabase("foo");
            var collection = database.GetCollection("bar");

            int index = 1000;
            for (int i = 0; i < index; i++)
            {
                BsonDocument bs = new BsonDocument
                {
                    {"hash", i},
                    {"Name", "Lennon" + (i).ToString()}
                };
                collection.Insert(bs);
            }

            var iList = collection.FindAll().ToList();
            var count = iList.Count();
            Console.WriteLine("Count: {0}", count);


        }
    }
}
