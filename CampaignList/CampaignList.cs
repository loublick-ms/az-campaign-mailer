using Azure.Messaging.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace CampaignList
{
    public static class CampaignList
    {
        private static ServiceClient dataverseClient = null;
        private static HashSet<string> alreadySentEmails = new();


        /// <summary>
        /// HTTP trigger for the CampaignList Durable Function.
        /// </summary>
        /// <param name="req">The HTTPRequestMessage containing the request content.</param>
        /// <param name="client">The Durable Function orchestration client.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns>The URLs to check the status of the function.</returns>
        [FunctionName("CampaignListHttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "campaign")] HttpRequestMessage req,
           [DurableClient] IDurableOrchestrationClient client,
           ILogger log)
        {
            // Get the campaign information from the HTTP request body
            CampaignConfiguration campaignConfig = await req.Content.ReadAsAsync<CampaignConfiguration>();

            // Function input comes from the request content.
            string instanceId = await client.StartNewAsync("CampaignListOrchestrator", campaignConfig);

            log.LogInformation($"Started orchestration with ID = '{instanceId}", instanceId);

            // Create the URL to allow the client to check status of a request (excluding the function key in the code querystring)
            string checkStatusUrl = string.Format("{0}://{1}:{2}/campaign/CampaignListHttpStart_Status?id={3}", req.RequestUri.Scheme, req.RequestUri.Host, req.RequestUri.Port, instanceId);

            // Create the response and add headers
            var response = new HttpResponseMessage()
            {
                StatusCode = System.Net.HttpStatusCode.Accepted,
                Content = new StringContent(checkStatusUrl),
            };
            response.Headers.Add("Location", checkStatusUrl);
            response.Headers.Add("Retry-After", "10");

            return response;
        }


        /// <summary>
        /// Orchestrates all activity functions for the campaign list queuing process.
        /// </summary>
        /// <param name="context">The context object of the orchestration.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns></returns>
        [FunctionName("CampaignListOrchestrator")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            [ServiceBus("myqueue", Connection = "ServiceBusConn")] IAsyncCollector<string> queueContacts,
            ILogger log)
        {
            log.LogInformation($"************** RunOrchestrator method executing ********************");

            // Retrive the campaign configuration object from the HTTP context
            CampaignConfiguration campaignConfig = context.GetInput<CampaignConfiguration>();

            alreadySentEmails = GetAlreadyMailedAddresses();

            // Create the Dataverse ServiceClient by connecting to the Dataverse database
            if (dataverseClient == null)
            {
                InitializeDataverseClient(log);
            }

            // Fan out the Dataverse query and queuing to run in parallel
            log.LogInformation($"************** Fanning out ********************");

            // Process the campaign list
            if (campaignConfig.ListName.Length > 0)
            {
                if (dataverseClient.IsReady)
                {
                    // Define the query attributes for pagination of the results.
                    // Set the number of records per page to retrieve.
                    int pageSize = campaignConfig.PageSize;

                    // Initialize the page number.
                    int pageNumber = 1;

                    // Specify the current paging cookie. For retrieving the first page, 
                    // pagingCookie should be null.
                    string pagingCookie = null;


                    // Determine if the list is dynamic or static
                    bool isDynamic = IsDynamic(campaignConfig.ListName);

                    // Get the query XML specific to dynamic and static lists
                    string queryXml;
                    if (isDynamic)
                    {
                        // Get the XML query
                        queryXml = GetDynamicQuery(campaignConfig.ListName);
                    }
                    else
                    {
                        // Retrieve the ID of the static campaign list
                        var listId = GetCampaignListID(campaignConfig.ListName);
                        queryXml = GetStaticQuery(listId);
                    }

                    int contactCount = 0;

                    // Process each page of the list query results until every page has been processed
                    bool morePages = true;
                    try
                    {
                        while (morePages)
                        {
                            // Add the pagination attributes to the XML query.
                            string currQueryXml = AddPaginationAttributes(queryXml, pagingCookie, pageNumber, pageSize);

                            // Excute the fetch query and get the results in XML format.
                            RetrieveMultipleRequest fetchRequest = new()
                            {
                                Query = new FetchExpression(currQueryXml)
                            };

                            log.LogInformation($"************** Fetching page {pageNumber} ********************");

                            EntityCollection pageCollection = ((RetrieveMultipleResponse)dataverseClient.Execute(fetchRequest)).EntityCollection;

                            // Convert EntityCollection to JSON serializable object collection.
                            List<CampaignContact> pageContactList = new();
                            if (pageCollection.Entities.Count > 0)
                            {
                                foreach (var contact in pageCollection.Entities)
                                {
                                    CampaignContact campaignContact = new();
                                    if (isDynamic)
                                    {
                                        campaignContact.EmailAddress = contact.Attributes["emailaddress1"].ToString();
                                        campaignContact.FullName = contact.Attributes["fullname"].ToString();
                                    }
                                    else
                                    {
                                        campaignContact.EmailAddress = ((AliasedValue)contact.Attributes["Contact.emailaddress1"]).Value.ToString();
                                        campaignContact.FullName = ((AliasedValue)contact.Attributes["Contact.fullname"]).Value.ToString();
                                    }
                                    campaignContact.MessageSubject = campaignConfig.MessageSubject;
                                    campaignContact.MessageBodyHtml = campaignConfig.MessageBodyHtml;
                                    campaignContact.MessageBodyPlainText = campaignConfig.MessageBodyPlainText;
                                    campaignContact.SenderEmailAddress = campaignConfig.SenderEmailAddress;
                                    campaignContact.ReplyToEmailAddress = campaignConfig.ReplyToEmailAddress;
                                    campaignContact.ReplyToDisplayName = campaignConfig.ReplyToDisplayName;

                                    // Add contact to contact list
                                    pageContactList.Add(campaignContact);
                                }
                            }

                            contactCount += pageContactList.Count;

                            // Start the new activity function and capture the task reference.
                            try
                            {
                                await QueueContacts(pageContactList, queueContacts, log);
                            }
                            catch (Exception ex)
                            {
                                log.LogError($"{ex}");
                            }

                            // Check for more records.
                            if (pageCollection.MoreRecords)
                            {
                                // Increment the page number to retrieve the next page.
                                pageNumber++;

                                // Set the paging cookie to the paging cookie returned from current results.                            
                                pagingCookie = pageCollection.PagingCookie;
                            }
                            else
                            {
                                morePages = false;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogError($"{ex}");

                    }

                    log.LogInformation($"Total number of contacts in the list: {contactCount}");
                    log.LogInformation($"Successfully completed processing {campaignConfig.ListName}");
                }
                else
                {
                    log.LogInformation($"A web service connection was not established. Campaign list {campaignConfig.ListName} was not processed");
                }
            }
            else
            {
                log.LogInformation("Campaign list name was empty. Please provide the name of a campaign list to process.");
            }

            // Wait until all the activity functions have done their work
            log.LogInformation($"************** 'Waiting' for parallel results ********************");
            log.LogInformation($"************** All activity functions complete ********************");

            return "Finished";
        }


        [FunctionName("QueryContactsActivity")]
        public static List<CampaignContact> QueryContacts(
            [ActivityTrigger] List<CampaignContact> contactList, ILogger log)
        {

            return contactList;
        }


        /// <summary>
        /// Queues campaign email list contacts in the Azure Storage queue.
        /// </summary>
        /// <param name="pageCollection">EntityCollection containing the list of CampaignContact objects</param>
        /// <param name="log">Logger object used to log messages to Log Analytics workspace</param>
        /// <returns></returns>
        //[FunctionName("QueueContactsActivity")]
        public static async Task QueueContacts(
            List<CampaignContact> contactList,
            IAsyncCollector<string> queueContacts,
            ILogger log)
        {
            // Iterate through EntityCollection and queue each campaign contact
            foreach (CampaignContact campaignContact in contactList)
            {
                if (IsAlreadyMailed(campaignContact.EmailAddress))
                {
                    continue;
                }

                // Convert campaign contact to JSON and add it to the Azure Storage queue
                string ccJson = JsonConvert.SerializeObject(campaignContact);
                try
                {
                    _ = queueContacts.AddAsync(ccJson);
                }
                catch (ServiceBusException sbex)
                {
                    log.LogError($"ServiceBusException: {sbex.Message}");

                    try
                    {
                        if (sbex.Reason == ServiceBusFailureReason.QuotaExceeded)
                        {
                            // Service Bus Queue Quota exceeded. Wait 5 minutes and try again.
                            await Task.Delay(300000);
                            await queueContacts.AddAsync(ccJson);
                        }
                        else if (sbex.Reason == ServiceBusFailureReason.ServiceBusy)
                        {
                            // Temporary error. Wait 10 seconds and try again.
                            await Task.Delay(10000);
                            await queueContacts.AddAsync(ccJson);
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogError($"{ex}");
                    }
                }
            }

            log.LogInformation($"Added {contactList.Count} to ServiceBus Queue.");
        }


        /// <summary>
        /// Create XML query that allows for paginated retrieval of campaign contacts.
        /// </summary>
        /// <param name="queryXml"></param>
        /// <param name="pageCookie"></param>
        /// <param name="pageNum"></param>
        /// <param name="recCount"></param>
        /// <returns></returns>
        public static string GetDynamicQuery(string listName)
        {
            // Return value containing the query XML
            string queryXml = string.Empty;

            // Query a campaign name and use it to derive the Dataverse query XML
            var query = new QueryByAttribute("list")
            {
                ColumnSet = new ColumnSet("query")
            };
            query.AddAttributeValue("listname", listName);

            var results = dataverseClient.RetrieveMultiple(query);
            queryXml = results.Entities.First().Attributes["query"].ToString();

            // Update the query XML to ensure it has the email address attribute 
            queryXml = AddEmailAttribute(queryXml);

            return queryXml;
        }


        /// <summary>
        /// Create XML query that allows for paginated retrieval of campaign contacts.
        /// </summary>
        /// <param name="queryXml"></param>
        /// <param name="pageCookie"></param>
        /// <param name="pageNum"></param>
        /// <param name="recCount"></param>
        /// <returns></returns>
        public static string GetStaticQuery(string listId)
        {
            // Return value containing the query XML
            string queryXml =
                $@" <fetch>
                    <entity name=""listmember"">
                        <attribute name=""entitytype"" />
                        <attribute name=""listmemberid"" />
                        <attribute name=""entityid"" />
                        <filter type=""and"">
                            <condition attribute=""listid"" operator=""eq"" value=""{listId}"" />
                        </filter>
                        <link-entity name=""contact"" from=""contactid"" to=""entityid"" alias=""Contact"">
                            <attribute name=""emailaddress1"" />
                            <attribute name=""fullname"" />
                        </link-entity>
                    </entity>
                </fetch>";

            return queryXml;
        }



        /// <summary>
        /// Add pagination attributes to the Dataverse query XML
        /// </summary>
        /// <param name="queryXml">Query XML for the list</param>
        /// <param name="pageCookie">Cookie used to mark the record that ended the previous page</param>
        /// <param name="pageNum">The page number of the previous page</param>
        /// <param name="pageSize">The number of records in each page</param>
        /// <returns></returns>
        public static string AddPaginationAttributes(string queryXml, string pageCookie, int pageNum, int pageSize)
        {
            StringReader stringReader = new StringReader(queryXml);
            XmlReader reader = XmlReader.Create(stringReader);          //new XmlTextReader(stringReader);

            // Load document
            XmlDocument doc = new XmlDocument();
            doc.Load(reader);

            XmlAttributeCollection attrs = doc.DocumentElement.Attributes;

            if (pageCookie != null)
            {
                XmlAttribute pagingAttr = doc.CreateAttribute("paging-cookie");
                pagingAttr.Value = pageCookie;
                attrs.Append(pagingAttr);
            }

            XmlAttribute pageAttr = doc.CreateAttribute("page");
            pageAttr.Value = System.Convert.ToString(pageNum);
            attrs.Append(pageAttr);

            XmlAttribute countAttr = doc.CreateAttribute("count");
            countAttr.Value = System.Convert.ToString(pageSize);
            attrs.Append(countAttr);

            StringBuilder sb = new StringBuilder(1024);
            StringWriter stringWriter = new StringWriter(sb);

            XmlTextWriter writer = new XmlTextWriter(stringWriter);
            doc.WriteTo(writer);
            writer.Close();

            return sb.ToString();
        }


        /// <summary>
        /// Determines if the selected campaign list is dynamic or static.
        /// </summary>
        /// <param name="listName"></param>
        /// <returns>true if dynamic or false if static.</returns>
        public static bool IsDynamic(string listName)
        {
            // Query a campaign name
            var query = new QueryByAttribute("list")
            {
                ColumnSet = new ColumnSet("type")
            };
            query.AddAttributeValue("listname", listName);

            var results = dataverseClient.RetrieveMultiple(query);
            return bool.Parse(results.Entities.First().Attributes["type"].ToString());
        }


        /// <summary>
        /// Retrieve the ID of a campaign list
        /// </summary>
        /// <param name="listName">Name of the list to retrieve the ID</param>
        /// <returns></returns>
        public static string GetCampaignListID(string listName)
        {
            // Query a campaign name
            var query = new QueryByAttribute("list")
            {
                ColumnSet = new ColumnSet("listid")
            };
            query.AddAttributeValue("listname", listName);

            var results = dataverseClient.RetrieveMultiple(query);
            return results.Entities.First().Attributes["listid"].ToString();
        }


        /// <summary>
        /// Add the email attribute to the campaign list query XML if it is not in the query already. 
        /// add the attribute.
        /// </summary>
        /// <param name="queryXml">The </param>
        /// <returns></returns> 
        public static string AddEmailAttribute(string queryXml)
        {
            var xDocument = XDocument.Parse(queryXml);

            // Find the contact entity node
            var entity = xDocument.Descendants("entity").Where(e => e?.Attribute("name").Value == "contact").First();

            // Does an email address attribute exist? If it doesn't, add it
            var emailAttributeExists = entity.Elements("attribute").Where(e => e?.Attribute("name").Value == "emailaddress1").Any();
            if (!emailAttributeExists)
            {
                entity.Add(new XElement("attribute", new XAttribute("name", "emailaddress1")));
            }

            // Return the udpated query XML
            return xDocument.ToString();
        }


        /// <summary>
        /// Connects to the campaign database using the Dataverse API.
        /// </summary>
        /// <returns>Returns a ServiceClient used to access and query the database.</returns>
        private static void InitializeDataverseClient(ILogger log)
        {
            // Dataverse environment URL and auth credentials.
            // Dataverse environment URL and login info.
            string url = Environment.GetEnvironmentVariable("DATAVERSE_URL");
            string appId = Environment.GetEnvironmentVariable("DATAVERSE_APPID");
            string secret = Environment.GetEnvironmentVariable("DATAVERSE_SECRET");



            // Create the Dataverse connection string
            string connectionString =
                $@"AuthType=ClientSecret;
                SkipDiscovery=true;url={url};
                Secret={secret};
                ClientId={appId};
                RequireNewInstance=true";

            //string connectionString = Environment.GetEnvironmentVariable("DataverseConn");

            try
            {
                // Create the Dataverse ServiceClient instance
                dataverseClient = new(connectionString);
                log.LogInformation("Successfully created the Dataverse service client");
            }
            catch (Exception ex)
            {
                log.LogInformation($"Exception thrown creating the Dataverse ServiceClient: {ex.Message}");
            }
        }

        private static HashSet<string> GetAlreadyMailedAddresses()
        {
            HashSet<string> addresses = new();
            string path = Path.Combine(Environment.CurrentDirectory, "AlreadyMailedAddresses.txt");
            using (StreamReader sr = new StreamReader(path))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    addresses.Add(line);
                }
            }
            return addresses;
        }

        private static bool IsAlreadyMailed(string emailAddress)
        {
            return alreadySentEmails.Contains(emailAddress);
        }
    }
}
