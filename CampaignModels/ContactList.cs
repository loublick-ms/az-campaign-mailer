using CampaignModels;
using System.Collections.Generic;

namespace CampaignList
{
    /// <summary>
    /// Attributes for the campaign list query
    /// </summary>
    public class ContactList
    {
        // Contact count
        public int ContactCount { get; set; }

        // The list of contacts for a fetch page
        public List<Contact> Contacts { get; set; }

        // Campaign list type flag
        public bool IsDynamic { get; set; }

        // More contacts flag
        public bool IsMoreContacts { get; set; }

        // Campaign list name
        public string ListName { get; set; }

        // Page number
        public int PageNumber { get; set; }

        // Campaign page size
        public int PageSize { get; set; }

        // Query fetch paging cookie
        public string PagingCookie { get; set; }

        // Query XML fetch statement
        public string QueryXml { get; set; }

        /// <summary>
        /// Empty constructor
        /// </summary>
        public ContactList()
        {
            ContactCount = 0;
            Contacts = new List<Contact>();
            IsDynamic = false;
            IsMoreContacts = true;
            ListName = string.Empty;
            PageNumber = 0;
            PageSize = 0;
            PagingCookie = string.Empty;
            QueryXml = string.Empty;
        }


        /// <summary>
        /// Constructor that accepts the campaign list name and query fetch paging size
        /// </summary>
        /// <param name="listName">Dataverse camapaign list name</param>
        /// <param name="pageSize">Dataverse query fetch page size</param>
        public ContactList(string listName, int pageSize)
        {
            this.ContactCount = 0;
            this.Contacts = new List<Contact>();
            this.IsDynamic = false;
            this.IsMoreContacts = true;
            this.ListName = listName;
            this.PageNumber = 0;
            this.PageSize = pageSize;
            this.PagingCookie = string.Empty;
            this.QueryXml = string.Empty;
        }


        /// <summary>
        /// Adds a contact to the contact list and increments the contact count
        /// </summary>
        /// <param name="contact"></param>
        public void AddContact(Contact contact)
        {
            this.Contacts.Add(contact);
            this.ContactCount++;
        }


        /// <summary>
        /// Removes the specified contact from the list
        /// </summary>
        /// <param name="contact"></param>
        public void RemoveContact(Contact contact) 
        { 
            this.Contacts.Remove(contact);
        }


        /// <summary>
        /// Increments the page number
        /// </summary>
        public void AddPage()
        {
            this.PageNumber++;
        }
    }
}
