using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CampaignModels
{
    public class CampaignManager
    {
        // Top-level property for the metrics JSON file
        public string CampaignRuns { get; set; } = "";

        //List of metrics for one or more campaigns
        public IList<CampaignMetrics>? campaignMetrics { get; set; } = new List<CampaignMetrics>();


        // Allows metrics for a single campaign to be added
        public void Add(CampaignMetrics metrics)
        {
            if (campaignMetrics != null)
            {
                campaignMetrics.Add(metrics);
            }
        }

        // Allows metrics for multiple campaigns to be added
        public void Add(IList<CampaignMetrics> metrics)
        {
            if (campaignMetrics != null)
            {
                foreach (CampaignMetrics m in metrics) 
                {
                    if (m != null)
                    {
                        campaignMetrics.Add((CampaignMetrics)m);
                    }
                }
                
            }
        }
    }
}
