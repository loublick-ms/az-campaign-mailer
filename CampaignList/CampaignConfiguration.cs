/*
 * CampaignList function helper class.
 */
namespace CampaignList
{
    /// <summary>
    /// Campaign configuration data used to encapsulate the HTTP message body data.
    /// </summary>
    public class CampaignConfiguration
    {
        public string ListName { get; set; }

        public int PageSize { get; set; }

        public string MessageSubject { get; set; }

        public string MessageBodyHtml { get; set; }

        public string MessageBodyPlainText { get; set; }

        public string SenderEmailAddress { get; set; }

        public string ReplyToEmailAddress { get; set; }

        public string ReplyToDisplayName { get; set; }
    }
}