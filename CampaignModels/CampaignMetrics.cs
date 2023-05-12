using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CampaignModels
{
    public class CampaignMetrics
    {
        // The name of the campaign list
        public string? CampaignList { get; set; }

        // Date and time the campaign run started
        public DateTime RunStart { get; set; }

        // Date and time the campaign run ended
        public DateTime RunEnd { get; set; }

        // Number of contacts retrieved from Dataverse
        public int RetrievedContacts { get; set; }

        // Number of contacts queued for email send
        public int QueuedContacts { get; set; }

        // Number of duplicate contacts processed
        public int DuplicateContacts { get; set; }

        // Number of contacts not sent an email
        public int UnsentContacts { get; set; }


        /// <summary>
        /// Empty constructor to initialize the instance with default values
        /// </summary>
        public CampaignMetrics()
        {
            this.Initialize();
        }


        /// <summary>
        /// Constructor to initialize the instance with a campaign list name
        /// </summary>
        /// <param name="listName">Campaign list name</param>
        public CampaignMetrics(string listName)
        {
            this.Initialize();
            this.CampaignList = listName;
        }


        /// <summary>
        /// Constructor to initialize the instance with all attributes
        /// </summary>
        /// <param name="listName">Campaign list name</param>
        /// <param name="runStart">Start date/time for the campaign run</param>
        /// <param name="runEnd">End date/time for the campaign run</param>
        /// <param name="retrContacts">Number of contacts retrieved from campaign list</param>
        /// <param name="queContacts">Number of contacts queued for email send</param>
        /// <param name="dupContacts">Number of duplicate contacts processed</param>
        /// <param name="unsentContacts">Number of contacts not sent an email</param>
        public CampaignMetrics(string listName, DateTime runStart, DateTime runEnd, int retrContacts, int queContacts, int dupContacts, int unsentContacts)
        {
            this.Initialize();
            this.CampaignList = listName;
            this.RunStart = runStart;
            this.RunEnd = runEnd;
            this.RetrievedContacts = retrContacts;
            this.DuplicateContacts = dupContacts;
            this.QueuedContacts = queContacts;
            this.UnsentContacts = unsentContacts;
        }


        // Initializes the instance
        private void Initialize()
        {
            this.CampaignList = null;
            this.RunStart = DateTime.MinValue;
            this.RunEnd = DateTime.MinValue;
            this.DuplicateContacts = 0;
            this.UnsentContacts = 0;
            this.QueuedContacts = 0;
            this.RetrievedContacts = 0;
        }
    }
}
