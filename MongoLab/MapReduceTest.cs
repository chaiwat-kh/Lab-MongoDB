using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Driver.Linq;
using MongoDB.Driver.Builders;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MongoLab
{
    [TestClass]
    public class MapReduceTest
    {
        [TestMethod]
        public void TestFindMapReduceData()
        {
            var connectionString = "mongodb://localhost:27017";
            var server = MongoServer.Create(connectionString);

            var db = server.GetDatabase("logs");
            var collection = db.GetCollection("logs_dcm");

            var where = Query.EQ("userName", "CARPASSAD\\Chaiwat.Kh");

            //var output = collection.AsQueryable().Take(400).ToList();
            var output = collection.Find(where).Take(10).ToList();
            foreach (var item in output)
            {
                Console.WriteLine(item["message"]);
                //Console.Write("[]");
                //foreach (var item2 in item)
                //{
                //    Console.Write(" |{0}:{1}", item2.Name.ToString(), item2.Value);
                //}
                //Console.WriteLine("");
            }
        }
        [TestMethod]
        public void TestReduceMachineName()
        {
            var connectionString = "mongodb://localhost:27017";
            var server = MongoServer.Create(connectionString);

            var db = server.GetDatabase("logs");
            var collection = db.GetCollection("logs_dcm");

            //var message = tojson('{method: 'dasdasd'}');
            var map = new BsonJavaScript(@"
                            function() {                                                                  
                                var strArr = this.machineName;
                                
                                emit(strArr, { totalTime: 1 });                                                            
                                }");
            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                var sum = 0;
                                var vocab = '';
                                for ( var i=0; i<values.length; i++ ) {
                                    sum += values[i].totalTime;
                                }
                                return {totalTime: sum, name: key};
                            }");
            var mapRet = collection.MapReduce(map, reduce);
            var ret = mapRet.GetResults().Take(10).ToList();
            ret.ForEach(r =>
            {
                var val = r["value"].AsBsonDocument;

                Console.WriteLine("[i] value: {0}, --- totalTime: {1}", val["name"], val["totalTime"]);
            });
        }

        [TestMethod]
        public void TestReduceMachineNameAndDateOri()
        {
            //var connectionString = "mongodb://localhost:27017";
            var connectionString = "mongodb://APPSIT01:27017";
            var server = MongoServer.Create(connectionString);

            var db = server.GetDatabase("logs");
            //var collection = db.GetCollection("logs_dcm");
            var collection = db.GetCollection("logs_gps_com");

            //var message = tojson('{method: 'dasdasd'}');
            var map = new BsonJavaScript(@"
                            function() {                       
                                var ts = this._id.getTimestamp();
                                var month = ts.getUTCFullYear() + '-' + (ts.getUTCMonth() + 1);
                                var stats = {};
                                var aDay = month + '-' + ts.getUTCDate();
                                stats[aDay] = 1;
                                
                                var strArr = this.machineName;
                                emit(strArr, stats);                                                            
                                }");
            var reduce = new BsonJavaScript(@"
                            function(key, values) {
                                function merge(out, stat) {
                                    for (var aDay in stat) {
                                        if( aDay != 'total' && aDay != 'totalDay')
                                        {
                                            out.total += stat[aDay];
                                             
                                            if (!stat.hasOwnProperty(aDay)) {
                                                continue;
                                            }
                                            
                                            out[aDay] = (out[aDay] || 0) + stat[aDay];
                                        }
                                    }                                        
                                }
                                
                                var out = {total: 0, totalDay:0};
                                for (var i=0; i < values.length; i++) {
                                    merge(out, values[i]);
                                }

                                return out;
                            }");

            var finalizef = new BsonJavaScript(@"
                            function(key, value)
                            {
                                value.average = value.total / 2; 
                                         
                                return value;
                            }");

            var options = new MapReduceOptionsBuilder();
            options.SetOutput(MapReduceOutput.Inline);

            //if (FinalizeExists(name))
            //{
            options.SetFinalize(finalizef);
            //}

            var mapRet = collection.MapReduce(map, reduce, options);
            var ret = mapRet.GetResults().ToList();

            int count = 0;
            ret.ForEach(r =>
            {
                Console.Write("[#]");
                foreach (var item2 in r)
                {
                    Console.WriteLine("{0}:{1} ", item2.Name.ToString(), item2.Value);
                }
                Console.WriteLine("");
            });

            Console.WriteLine("Total: {0}", count);
        }

        [TestMethod]
        public void TestReduceMachineNameAndDate()
        {
            var connectionString = "mongodb://localhost:27017";
            //var connectionString = "mongodb://APPSIT01:27017";

            var server = MongoServer.Create(connectionString);
            var db = server.GetDatabase("logs");
            var collection = db.GetCollection("logs_dcm");

            //var collection = db.GetCollection("logs_gps_com");
            //var strArr = this.machineName + '-' + field;
            var map = new BsonJavaScript(@"
                            function() {                       
                                var ts = this._id.getTimestamp();
                                var month = ts.getUTCFullYear() + '-' + (ts.getUTCMonth() + 1);
                                var stats = {};
                                var field = month + '-' + ts.getUTCDate();
                                stats[field] = 1;

                                var name = this.machineName;
                                var date = field;
                                emit({name:name,date:date}, {count: 1, date: field});                                                            
                                }");
            var reduce = new BsonJavaScript(@"
                            function(key, values) {                                
                                var sum = 0;
                                var date = '';
                                values.forEach(function(doc) {
                                    sum += doc.count;
                                    date = doc.date;
                                  });
                                return {date: date, count: sum};
                            }");
            var finalizef = new BsonJavaScript(@"
                            function(key, value)
                            {
                                value.average = value.count / 2; 
                                         
                                return value;
                            }");

            var options = new MapReduceOptionsBuilder();
            options.SetOutput(MapReduceOutput.Inline);
            options.SetFinalize(finalizef);

            //var query = new QueryDocument("x", "$lt:3");
            //var query2 = Query.LT("a", 3);
            var mapRet = collection.MapReduce( map, reduce, options);
            var ret = mapRet.GetResults().ToList();

            
            int count = 0;
            ret.ForEach(r =>
            {
                Console.Write("[#]");
                foreach (var item2 in r)
                {
                    if (item2.Name.Equals("value"))
                    {
                        var obj = JsonConvert.DeserializeObject<JToken>(item2.Value.ToString());                     
                        var v = obj["count"].Value<int>();
                        count += v;
                        Console.Write("{0}:{1} ", item2.Name.ToString(), item2.Value);
                    }
                    
                }
                Console.WriteLine("");
            });

            Console.WriteLine("Total: {0}", count);
        }

        [TestMethod]
        public void TestReduceDateAndMethod()
        {
            var connectionString = "mongodb://localhost:27017";
            //var connectionString = "mongodb://APPSIT01:27017";

            var server = MongoServer.Create(connectionString);
            var db = server.GetDatabase("logs");
            var collection = db.GetCollection("logs_dcm");

            //var collection = db.GetCollection("logs_gps_com");
            //var strArr = this.machineName + '-' + field;
            var map = new BsonJavaScript(@"
                            function() {                       
                                var date = new Date( this.timestamp.getFullYear(), 
                                                     this.timestamp.getMonth(),
                                                     this.timestamp.getDay() );
                                var out = { methods: {}, userNames: {} }
                                var msg = JSON.parse(this.message);
                                out.methods[msg.method] = 1;
                                out.userNames[this.userName] = 1;                       
                                
                                emit(date, out);                                                            
                                }");
            var reduce = new BsonJavaScript(@"
                            function(key, values) {                                
                                var out = { methods: {}, userNames: {} }
                                values.forEach(function(value) {
                                    for( var method in value.methods ){
                                        if( out.methods[method] ){
                                            out.methods[method] += value[method];                                            
                                        }
                                        else{
                                            out.methods[method] = value[method];
                                        }
                                    };
                                    for( var userName in value.userNames ){
                                        if( out.userNames[userName] ){
                                            out.userNames[userName] += value[userName];
                                        }
                                        else{
                                            out.methods[userName] = value[userName];
                                        }
                                    }
                                });
                                return out;
                            }");
            var finalizef = new BsonJavaScript(@"
                            function(key, value)
                            {                                  
                                return value;
                            }");

            var options = new MapReduceOptionsBuilder();
            options.SetOutput(MapReduceOutput.Inline);
            options.SetFinalize(finalizef);
            var mapRet = collection.MapReduce(map, reduce, options);
            var ret = mapRet.GetResults().ToList();

            int count = 0;
            ret.ForEach(r =>
            {
                Console.Write("[#]");
                foreach (var item2 in r)
                {
                    if (item2.Name.Equals("value"))
                    {
                        var obj = JsonConvert.DeserializeObject<JToken>(item2.Value.ToString());
                        var v = obj["count"].Value<int>();
                        count += v;
                        Console.Write("{0}:{1} ", item2.Name.ToString(), item2.Value);
                    }

                }
                Console.WriteLine("");
            });

            Console.WriteLine("Total: {0}", count);
        }
    }
}
