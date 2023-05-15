using System.Text.Json;

/*
 * CampaignList Durable Function helper class.
 */
namespace CampaignModels
{
    /// <summary>
    /// Campaign contact information class used to pass campaign and member contact
    /// data to CampaignMailer Azure functions.
    /// </summary>
    public class CampaignContact
    {
        public string? FullName { get; set; }

        public string? EmailAddress { get; set; }

        public string? ReplyToEmailAddress { get; set; }

        public string? ReplyToDisplayName { get; set; }

        public string? MessageSubject { get; set; }

        public string? MessageBodyHtml { get; set; }

        public string? MessageBodyPlainText { get; set; }

        public string? SenderEmailAddress { get; set; }

    }
}