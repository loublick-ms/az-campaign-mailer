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
    public class Contact
    {
        public string? FullName { get; set; }

        public string? EmailAddress { get; set; }
    }
}