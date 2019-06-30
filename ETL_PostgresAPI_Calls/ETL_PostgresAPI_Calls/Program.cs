using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
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
            }
            return accessToken;
        }
        public static void ETL(string authToken, KeyValuePair<string, string> tableInfo)
        {
            try
            {
                string sourceUrl = "APIRequestURL";
                //string sourceUrl = Dts.Variables["User::SourceURL"].Value.ToString();
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
                        case "PurchaseOrderUpdateStatus":
                            //List<PurchaseOrderUpdateStatus> PurchaseOrderUpdateStatuss = objects.Select(x => JsonConvert.DeserializeObject<PurchaseOrderUpdateStatus>(x.ToString())).ToList();
                            //addRecordsPurchaseOrderUpdateStatus(PurchaseOrderUpdateStatuss);
                            break;
                        case "PurchaseOrder":
                            //List<PurchaseOrder> PurchaseOrders = objects.Select(x => JsonConvert.DeserializeObject<PurchaseOrder>(x.ToString())).ToList();
                            //addRecordsPurchaseOrder(PurchaseOrders);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            //Dts.TaskResult = (int)ScriptResults.Success;
        }
    }
}
