1. In the SQL query in the CTE, I combined data from the facebook_ads_basic_daily, facebook_adset and facebook_campaign tables using a LEFT JOIN to get a table that will contain:
- ad_date - the date the ad was shown on Facebook
- campaign_name - the name of the campaign on Facebook
- adset_name - the name of the ad set on Facebook
- spend, impressions, reach, clicks, leads, value  - campaign and ad set metrics on the corresponding days.
2. Using UNION ALL, combined data from the google_ads_basic_daily table and the first CTE to get a single table with information about Facebook and Google marketing campaigns.
3. Similarly to the previous task, from the resulting combined table (CTE), I selected:
- ad_date - date of advertisement display
- media_source - name of the procurement source (Google Ads / Facebook Ads) - I created this column myself
- campaign_name - campaign name
- adset_name - ad set name
- values ​​aggregated by date and campaign and ad set name for the following indicators:
total cost,
number of impressions,
number of clicks,
total conversion value.

To accomplish this task, I grouped the table by the fields ad_date, media_source, campaign_name, and adset_name.

4. By combining data from four tables, I identified the campaign with the highest ROMI among all campaigns with a total spend of more than 500,000.

5. Within this campaign, I identified the ad set (adset_name) with the highest ROMI.
