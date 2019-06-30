using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace ETL_PostgresAPI_Calls
{
    class Program
    {
        static void Main(string[] args)
        {
            string username = "Test";
            string password = "Test";

            string myJsonString1 = "{\"dataSource\":\"PurchaseOrderUpdate\",\"columns\":[\"purchaseorderupdatestatus_id\",\"statustype\",\"statustext\"]}";
            string myJsonString2 = "{\"dataSource\":\"PurchaseOrder\",\"columns\":[\"purchaseorder_id\",\"originalpurchaseorderkey\"]}";

            Dictionary<string, string> tableInfoCollection = new Dictionary<string, string>();
            tableInfoCollection.Add("PurchaseOrderUpdate", myJsonString1);
            tableInfoCollection.Add("PurchaseOrder", myJsonString2);

            string authToken = Authenticate(username, password);
            if (!string.IsNullOrEmpty(authToken))
            {
                foreach (KeyValuePair<string, string> keyValuePair in tableInfoCollection)
                {
                    ETL(authToken, keyValuePair);
                }
            }

            else
            {
                Console.WriteLine("Failed to Authenticate!");

                //For making calls through SSIS
                //Dts.TaskResult = (int)ScriptResults.Failure;
            }

        }

        public static string Authenticate(string username, string password)
        {
            string accessToken = string.Empty;
            try
            {
                ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;

                var client = new RestClient("AuthenticationRequestURL");
                var request = new RestRequest(Method.POST);
                request.AddHeader("cache-control", "no-cache");
                request.AddHeader("content-type", "application/x-www-form-urlencoded");
                request.AddParameter("application/x-www-form-urlencoded", "connectionString&username=" + username + "&password=" + password + "", ParameterType.RequestBody);

                IRestResponse response = client.Execute(request);
                accessToken = JObject.Parse(response.Content)["access_token"].ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                //For making calls through SSIS
                //Dts.TaskResult = (int)ScriptResults.Failure;
            }
            return accessToken;
        }
        public static void ETL(string authToken, KeyValuePair<string, string> tableInfo)
        {
            try
            {
                string sourceUrl = "APIRequestURL";
                //string sourceUrl = Dts.Variables["User::SourceURL"].Value.ToString();     // For SSIS 
                HttpClient client = new HttpClient();
                client.BaseAddress = new Uri(sourceUrl);
                // Add an Accept header for JSON format.  
                client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", authToken);
                var buffer = System.Text.Encoding.UTF8.GetBytes(tableInfo.Value);
                var byteContent = new ByteArrayContent(buffer);
                byteContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                string APIUrl = string.Format(sourceUrl);
                var response = client.PostAsync(APIUrl, byteContent).Result;

                if (response.IsSuccessStatusCode)
                {
                    var returnValue = JObject.Parse(response.Content.ReadAsStringAsync().Result)["result"]["result"].ToString();
                    var objects = JsonConvert.DeserializeObject<List<object>>(returnValue);

                    switch (tableInfo.Key)
                    {
                        case "PurchaseOrderUpdate":
                            List<PurchaseOrderUpdate> PurchaseOrderUpdates = objects.Select(x => JsonConvert.DeserializeObject<PurchaseOrderUpdate>(x.ToString())).ToList();
                            addRecordsPurchaseOrderUpdate(PurchaseOrderUpdates);
                            break;
                        case "PurchaseOrder":
                            List<PurchaseOrder> PurchaseOrders = objects.Select(x => JsonConvert.DeserializeObject<PurchaseOrder>(x.ToString())).ToList();
                            //addRecordsPurchaseOrder(PurchaseOrders);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                //For making calls through SSIS
                //Dts.TaskResult = (int)ScriptResults.Failure;
            }
            //Dts.TaskResult = (int)ScriptResults.Success;
        }

        public class PurchaseOrderUpdate
        {
            public int purchaseorderupdatestatus_id { get; set; }
            public string statustype { get; set; }
            public string statustext { get; set; }
        }

        public class PurchaseOrder
        {
            public int purchaseorder_id { get; set; }
            public string originalpurchaseorderkey { get; set; }
           
        }

        public static void addRecordsPurchaseOrderUpdate(List<PurchaseOrderUpdate> Data)
        {
            string connetionString = null;
            string sql = null;
            //string reportDB = Dts.Variables["User::ReportDB"].Value.ToString();
            //string reportServer = Dts.Variables["User::ReportServer"].Value.ToString();

            //connetionString = @"Server= {reportServer}; Database= {reportDB}; Integrated Security=SSPI;";
            string reportServer = "reportServerName";
            string reportDB = "reportDBInstanceName";
            connetionString = String.Format("Server= {0}; Database= {1}; Integrated Security=SSPI;", reportServer, reportDB);
            using (SqlConnection cnn = new SqlConnection(connetionString))
            {
                sql = "insert into Staging.PurchaseOrderUpdate([purchaseorderupdatestatus_id],[statustype],[statustext]) values(@purchaseorderupdatestatus_id,@statustype, @statustext)";
                cnn.Open();
                foreach (var data in Data)
                {
                    using (SqlCommand cmd = new SqlCommand(sql, cnn))
                    {

                        cmd.Parameters.AddWithValue("@purchaseorderupdatestatus_id", data.purchaseorderupdatestatus_id);
                        cmd.Parameters.AddWithValue("@statustype", DBNull.Value);
                        cmd.Parameters.AddWithValue("@statustext", data.statustext);
                        cmd.ExecuteNonQuery();

                    }
                }
            }
        }
        public static void addRecordsPurchaseOrder(List<PurchaseOrder> Data)
        {
            string connetionString = null;
            string sql = null;
            string reportServer = "reportServerName";
            string reportDB = "reportDBInstanceName";
            //string reportDB = Dts.Variables["User::ReportDB"].Value.ToString();
            //string reportServer = Dts.Variables["User::ReportServer"].Value.ToString();

            //connetionString = @"Server= {reportServer}; Database= {reportDB}; Integrated Security=SSPI;";
            connetionString = String.Format("Server= {0}; Database= {1}; Integrated Security=SSPI;", reportServer, reportDB);
            using (SqlConnection cnn = new SqlConnection(connetionString))
            {
                sql = "insert into Staging.PurchaseOrder ([purchaseorder_id],[originalpurchaseorderkey]) values(@purchaseorder_id,@originalpurchaseorderkey)";
                cnn.Open();
                foreach (var data in Data)
                {
                    using (SqlCommand cmd = new SqlCommand(sql, cnn))
                    {

                        cmd.Parameters.AddWithValue("@purchaseorderid", data.purchaseorder_id);
                       
                        if (string.IsNullOrEmpty(data.originalpurchaseorderkey))
                        {
                            cmd.Parameters.AddWithValue("@originalpurchaseorderkey", DBNull.Value);

                        }
                        else
                        {
                            cmd.Parameters.AddWithValue("@originalpurchaseorderkey", data.originalpurchaseorderkey);

                        }
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

    }
}
