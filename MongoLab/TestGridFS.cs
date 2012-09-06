using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using MongoDB.Driver.GridFS;
using System.IO;

namespace MongoLab
{
    [TestClass]
    public class TestGridFS
    {
        [TestMethod]
        public void TestUploadStreamFile2GridFS()
        {
            var server = MongoServer.Create("mongodb://localhost:27017");
            var db = server.GetDatabase("dcm");

            FileStream fs = new FileStream(@"D:\Download\FileTestUpload\Hello2.txt", FileMode.Open, FileAccess.Read);
            System.IO.Stream strm = fs;

            MongoGridFS file = new MongoGridFS(db);
            MongoGridFSCreateOptions op = new MongoGridFSCreateOptions();                        
            op.Aliases = new string[]{"Home"};            

            var upFile = file.Upload(strm, "SayHi", op);
            Console.WriteLine("MD5: {0}", upFile.MD5);

        }

        [TestMethod]
        public void TestUploadArrayFile2GridFS()
        {
            var server = MongoServer.Create("mongodb://localhost:27017");
            var db = server.GetDatabase("dcm");

            FileStream fs = new FileStream(@"D:\Download\FileTestUpload\Hello2.txt", FileMode.Open, FileAccess.Read);
            System.IO.Stream strm = fs;

            List<byte> readByte = new List<byte>();
            int bufferSize = 4048;
            byte[] buffer = new byte[bufferSize];
            int readCount = strm.Read(buffer, 0, bufferSize);
            while (readCount > 0)
            {
                readByte.AddRange(buffer.Take(readCount).ToArray());
                readCount = strm.Read(buffer, 0, bufferSize);
            }
            var micBytes = readByte.ToArray();

            Stream stream = new MemoryStream(micBytes);

            MongoGridFS file = new MongoGridFS(db);
            var upFile = file.Upload(stream, "ArrayHi");
            Console.WriteLine("MD5: {0}", upFile.MD5);

        }

        [TestMethod]
        public void TestReadGridFS2Array()
        {
            var server = MongoServer.Create("mongodb://localhost:27017");
            var db = server.GetDatabase("dcm");

            var where = Query.EQ("md5", "cac1807172774be5d48937cee90c9e84");

            MongoGridFS fs = db.GridFS;
            MongoGridFSFileInfo fileInfo = fs.FindOne(where);
            MongoGridFSStream readerFS = fileInfo.Open(System.IO.FileMode.Open);
            List<byte> readByte = new List<byte>();
            int bufferSize = 4048;
            byte[] buffer = new byte[bufferSize];
            int readCount = readerFS.Read(buffer, 0, bufferSize);
            while (readCount > 0)
            {
                readByte.AddRange(buffer.Take(readCount).ToArray());
                readCount = readerFS.Read(buffer, 0, bufferSize);
            }     

        }
        
        [TestMethod]
        public void TestReadAllGridFS2TextAndAddText()
        {
            var server = MongoServer.Create("mongodb://localhost:27017");
            var db = server.GetDatabase("dcm");
            var gridSettings = new MongoGridFSSettings();
            MongoGridFS fs = db.GetGridFS(gridSettings);
            var fileInfo = fs.Find(Query.EQ("filename", "D:\\Download\\FileTestUpload\\Hello2.txt"));
                      
            int index = 0;
            foreach (var item in fileInfo)
            {
                MongoGridFSStream readerFS = item.Open(System.IO.FileMode.Open);               
                
                StreamReader reader = new StreamReader(readerFS);
                string text = reader.ReadToEnd();
                Console.WriteLine("####### {0}",  ++index);
                Console.WriteLine(text);

                byte[] byteArray = Encoding.ASCII.GetBytes(" bon");
                readerFS.Write(byteArray, 0, byteArray.Count());
                readerFS.Close();
            }            
        }
        [TestMethod]
        public void TestReadAllGridFS2Text()
        {
            var server = MongoServer.Create("mongodb://localhost:27017");
            var db = server.GetDatabase("dcm");
            var gridSettings = new MongoGridFSSettings();
            MongoGridFS fs = db.GetGridFS(gridSettings);
            var fileInfo = fs.Find(Query.EQ("filename", "D:\\Download\\FileTestUpload\\Hello2.txt"));

            int index = 0;
            foreach (var item in fileInfo)
            {
                MongoGridFSStream readerFS = item.Open(System.IO.FileMode.Open);

                StreamReader reader = new StreamReader(readerFS);
                string text = reader.ReadToEnd();
                Console.WriteLine("####### {0}", ++index);
                Console.WriteLine(text);                
            }
        }

        [TestMethod]
        public void TestRemoveAllGFSandDelete()
        {
            var server = MongoServer.Create("mongodb://localhost:27017");
            var db = server.GetDatabase("dcm");
            var gridSettings = new MongoGridFSSettings();
            MongoGridFS fs = db.GetGridFS(gridSettings);
            fs.Files.RemoveAll();

        }
    }
}
