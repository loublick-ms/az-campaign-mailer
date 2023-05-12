using CampaignModels;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
//using Newtonsoft.Json;
//using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using static CampaignModels.CampaignManager;

namespace CampaignList
{
    [StorageAccount("AzureWebJobsStorage")]
    public static class CampaignList
    {
        // Dataverse client
        private static ServiceClient dataverseClient = null;

        // Retrieved campaign contacts
        private static HashSet<string> retrievedEmails;

        // Retrieved campaign contacts
        private static HashSet<string> duplicateEmails;

        // Retrieved campaign contacts
        private static HashSet<string> queuedEmails;

        // Retrieved campaign contacts
        private static HashSet<string> unsentEmails;


        /// <summary>
        /// HTTP trigger for the CampaignList Durable Function.
        /// </summary>
        /// <param name="req">The HTTPRequestMessage containing the request content.</param>
        /// <param name="client">The Durable Function orchestration client.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns>The URLs to check the status of the function.</returns>
        [FunctionName("CampaignListHttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "campaign")] HttpRequestMessage req,
           [Blob("campaign-mailer/campaign-mailer-config.json", FileAccess.Write)] Stream configStream,
           [Blob("campaign-mailer/campaign-metrics.json", FileAccess.Read)] string inMetricsStr,
           [Blob("campaign-mailer/campaign-metrics.json", FileAccess.Write)] Stream outMetricsStream,
           [Queue("%QueueName%")] ICollector<Contact> queueContacts,
           ILogger log)
        {
            // Initialize member collections
            retrievedEmails = new HashSet<string>();
            queuedEmails = new HashSet<string>();
            duplicateEmails = new HashSet<string>();
            unsentEmails = new HashSet<string>();
            
            // Get start date/time
            DateTime runStart = DateTime.Now;

            // Get campaign information from the HTTP request body and save to blob storage
            CampaignConfiguration campaignConfig = await req.Content.ReadAsAsync<CampaignConfiguration>();
            string ccStr = JsonSerializer.Serialize(campaignConfig);
            byte[] ccBytes = Encoding.UTF8.GetBytes(ccStr);
            await configStream.WriteAsync(ccBytes, 0, ccBytes.Length);
            configStream.Flush();

            // Fetch the contact list from Dataverse and queue the contacts
            ContactList contactList = new ContactList(campaignConfig.ListName, campaignConfig.PageSize);
            while (contactList.IsMoreContacts)
            {
                CreateContactList(contactList, queueContacts, log);
            }

            // Get end date/time
            DateTime runEnd = DateTime.Now;

            // Write the campaign metrics to blob storage
            CampaignMetrics campaignMetrics = new(contactList.ListName, runStart, runEnd, retrievedEmails.Count, queuedEmails.Count, duplicateEmails.Count, unsentEmails.Count);
            FormatMetrics(campaignMetrics, inMetricsStr, outMetricsStream, log);

            // Return HTTP response
            return req.CreateResponse(System.Net.HttpStatusCode.Accepted);
        }


        public static void FormatMetrics(CampaignMetrics currMetrics, string prevMetrics, Stream outStream, ILogger log)
        {
            // Create the primary instance used to store metrics
            CampaignManager cm = new();

            // Format the metrics for the current run
            if (currMetrics != null)
            {
                // Format the current metrics
                cm.campaignMetrics.Add(currMetrics);
            }
            else
            {
                log.LogError($"************** FormatMetrics: Error: Current metrics were not available **************");
            }

            // Add the metrics from the previous runs
            if (prevMetrics != null) 
            {
                CampaignManager prevManager = JsonSerializer.Deserialize<CampaignManager>(prevMetrics);
                foreach (CampaignMetrics metricsRun in prevManager.campaignMetrics)
                {
                    cm.campaignMetrics.Add(metricsRun);
                }
            }

            log.LogInformation($"************** FormatMetrics: Storing metrics run history **************");

            JsonSerializerOptions options = new() { WriteIndented = true };
            string metricsStr = JsonSerializer.Serialize(cm, options);
            byte[] metricsBytes = Encoding.UTF8.GetBytes(metricsStr);
            outStream.Write(metricsBytes);
            outStream.Flush();
        }


        /// <summary>
        /// Fetches contacts from the Dataverse campaign list and build a contact list.
        /// </summary>
        /// <param name="context">The context object of the orchestration.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns></returns>
        public static void CreateContactList(ContactList contactList,
            [Queue("q-sa-campaign-mailer")] ICollector<Contact> queueContacts,
            ILogger log)
        {
            log.LogInformation($"************** CreateContactList: Method Executing ********************");

            // Create the Dataverse ServiceClient by connecting to the Dataverse database
            if (dataverseClient == null)
            {
                InitializeDataverseClient(log);
            }

            // Process the campaign list
            if (contactList.ListName.Length > 0)
            {
                if (dataverseClient.IsReady)
                {
                    // Initialize the page number.
                    contactList.AddPage();

                    // Determine if the list is dynamic or static
                    contactList.IsDynamic = IsDynamic(contactList.ListName);

                    // Get the query XML specific to dynamic or static lists
                    if (contactList.IsDynamic)
                    {
                        // Get the XML query
                        contactList.QueryXml = GetDynamicQuery(contactList.ListName);
                    }
                    else
                    {
                        // Retrieve the ID of the static campaign list
                        var listId = GetCampaignListID(contactList.ListName);
                        contactList.QueryXml = GetStaticQuery(listId);
                    }

                    // Process each page of the list query results until every page has been processed
                    try
                    {
                        while (contactList.IsMoreContacts)
                        {
                            // Add the pagination attributes to the XML query.
                            string currQueryXml = AddPaginationAttributes(contactList);

                            // Excute the fetch query and get the results in XML format.
                            RetrieveMultipleRequest fetchRequest = new()
                            {
                                Query = new FetchExpression(currQueryXml)
                            };

                            
                            log.LogInformation($"************** CreateContactList: Fetching Page {contactList.PageNumber} ********************");

                            EntityCollection pageCollection = ((RetrieveMultipleResponse)dataverseClient.Execute(fetchRequest)).EntityCollection;

                            // Convert EntityCollection to JSON serializable object collection.
                            if (pageCollection.Entities.Count > 0)
                            {
                                foreach (var pageContact in pageCollection.Entities)
                                {
                                    Contact campaignContact = new();
                                    if (contactList.IsDynamic)
                                    {
                                        campaignContact.EmailAddress = pageContact.Attributes["emailaddress1"].ToString();
                                        campaignContact.FullName = pageContact.Attributes["fullname"].ToString();
                                    }
                                    else
                                    {
                                        campaignContact.EmailAddress = ((AliasedValue)pageContact.Attributes["Contact.emailaddress1"]).Value.ToString();
                                        campaignContact.FullName = ((AliasedValue)pageContact.Attributes["Contact.fullname"]).Value.ToString();
                                    }

                                    // Add email to the contact list
                                    if (!retrievedEmails.Contains(campaignContact.EmailAddress))
                                    {
                                        // Add contact to contact list
                                        contactList.AddContact(campaignContact);
                                        retrievedEmails.Add(campaignContact.EmailAddress);

                                        // Add contact to stroage queue
                                        queueContacts.Add(campaignContact);
                                        queuedEmails.Add(campaignContact.EmailAddress);
                                    }
                                    else
                                    {
                                        duplicateEmails.Add(campaignContact.EmailAddress);
                                    }
                                }
                                log.LogInformation($"************** CreateContactList: Successfully Queued Contacts: {queuedEmails.Count} **************");
                            }

                            // Check for more records.
                            if (pageCollection.MoreRecords)
                            {
                                // Increment the page number to retrieve the next page.
                                contactList.AddPage();

                                // Set the paging cookie to the paging cookie returned from current results.                            
                                contactList.PagingCookie = pageCollection.PagingCookie;
                            }
                            else
                            {
                                // Set the more records flag to false to end the loop
                                contactList.IsMoreContacts = false;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogError($"************** CreateContactList: Error creating contact list {ex} **************");
                    }

                    log.LogInformation($"************** CreateContactList: Successfully completed processing Campaign: {contactList.ListName} **************");
                    log.LogInformation($"************** CreateContactList: Total number of contacts: {contactList.ContactCount} **************");
                }
                else
                {
                    log.LogInformation($"Dataverse connection was not established. Campaign list {contactList.ListName} was not processed");
                }
            }
            else
            {
                contactList.IsMoreContacts = false;
                log.LogInformation("Campaign list name was empty. Please provide the name of a campaign list to process.");
            }
            //return Task.FromResult(contactList);
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
        public static string AddPaginationAttributes(ContactList contactList)
        {
            StringReader stringReader = new StringReader(contactList.QueryXml);
            XmlReader reader = XmlReader.Create(stringReader);          //new XmlTextReader(stringReader);

            // Load document
            XmlDocument doc = new XmlDocument();
            doc.Load(reader);

            XmlAttributeCollection attrs = doc.DocumentElement.Attributes;

            if (contactList.PagingCookie != null)
            {
                XmlAttribute pagingAttr = doc.CreateAttribute("paging-cookie");
                pagingAttr.Value = contactList.PagingCookie;
                attrs.Append(pagingAttr);
            }

            XmlAttribute pageAttr = doc.CreateAttribute("page");
            pageAttr.Value = System.Convert.ToString(contactList.PageNumber);
            attrs.Append(pageAttr);

            XmlAttribute countAttr = doc.CreateAttribute("count");
            countAttr.Value = System.Convert.ToString(contactList.PageSize);
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
            // Dataverse environment URL and login info.
            string connectionString = Environment.GetEnvironmentVariable("DataverseConn");
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
    }
}
