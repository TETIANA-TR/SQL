-- Перший CTE: З'єднуємо дані з таблиць facebook_ads_basic_daily, facebook_adset та facebook_campaign
WITH facebook_ads AS (
    SELECT 
        facebook_ads_basic_daily.ad_date        -- Дата показу реклами
        , facebook_campaign.campaign_name       -- Назва кампанії на Facebook
        , facebook_adset.adset_name             -- Назва набору оголошень на Facebook 
                
        -- Метрики кампаній на Facebook
        , facebook_ads_basic_daily.spend
        , facebook_ads_basic_daily.impressions
        , facebook_ads_basic_daily.reach
        , facebook_ads_basic_daily.clicks
        , facebook_ads_basic_daily.leads
        , facebook_ads_basic_daily.value
        
        -- З'єднуємо таблиці за adset_id та campaign_id
        FROM facebook_ads_basic_daily
    LEFT JOIN facebook_adset 
        ON facebook_ads_basic_daily.adset_id = facebook_adset.adset_id           -- З'єднуємо через adset_id
    LEFT JOIN facebook_campaign 
        ON facebook_ads_basic_daily.campaign_id = facebook_campaign.campaign_id  -- З'єднуємо через campaign_id
),
-- Другий CTE: Об'єднуємо дані з Facebook Ads та Google Ads
combined_ads AS (
    -- Вибірка для Facebook Ads
    SELECT 
        facebook_ads.ad_date             -- Дата показу реклами
        , facebook_ads.campaign_name     -- Назва кампанії
        , facebook_ads.adset_name        -- Назва набору оголошень
        , 'Facebook Ads' AS media_source -- Джерело медіа (Facebook)
        , facebook_ads.spend             -- Витрати на рекламу
        , facebook_ads.impressions       -- Кількість показів
        , facebook_ads.clicks            -- Кількість кліків
        , facebook_ads.value             -- Загальна вартість конверсій
    FROM facebook_ads

    UNION ALL  -- Об'єднуємо з Google Ads, використовуючи UNION ALL для збереження всіх рядків (включаючи дублікати)

    -- Вибірка для Google Ads
    SELECT 
        google_ads_basic_daily.ad_date          -- Дата показу реклами
        , google_ads_basic_daily.campaign_name  -- Назва кампанії
        , NULL AS adset_name                    -- Для Google Ads не використовуємо adset, тому ставимо NULL
        , 'Google Ads' AS media_source          -- Джерело медіа (Google)
        , google_ads_basic_daily.spend          -- Витрати на рекламу
        , google_ads_basic_daily.impressions    -- Кількість показів
        , google_ads_basic_daily.clicks         -- Кількість кліків
        , google_ads_basic_daily.value          -- Загальна вартість конверсій
    FROM google_ads_basic_daily
)
-- Вибірка з об'єднаної таблиці
SELECT
    combined_ads.ad_date          -- Дата показу реклами
    , combined_ads.media_source   -- Джерело медіа (Facebook Ads або Google Ads)
    , combined_ads.campaign_name  -- Назва кампанії
    ,
    CASE 
        WHEN combined_ads.adset_name IS NULL THEN 'N/A'    -- Якщо adset_name є NULL, то замінюємо на 'N/A'
        ELSE combined_ads.adset_name                       -- Якщо adset_name не NULL, то залишаємо його значення
    END AS adset_name                                      -- Створюємо колонку adset_name
    , SUM(combined_ads.spend) AS total_spend               -- Загальна сума витрат
    , SUM(combined_ads.impressions) AS total_impressions   -- Загальна кількість показів
    , SUM(combined_ads.clicks) AS total_clicks             -- Загальна кількість кліків
    , SUM(combined_ads.value) AS total_value               -- Загальна вартість конверсій
FROM combined_ads

-- Групуємо дані за датою, медіа-джерелом, назвою кампанії та набором оголошень
GROUP BY combined_ads.ad_date
	, combined_ads.media_source
	, combined_ads.campaign_name
	, combined_ads.adset_name
	
-- Сортуємо результат за датою показу, медіа-джерелом та назвою кампанії
ORDER BY combined_ads.ad_date
	, combined_ads.media_source
    , combined_ads.campaign_name;



      --ДОДАТКОВЕ ЗАВДАННЯ

WITH facebook_ads_data AS (
    -- Об'єднання даних з таблиць Facebook Ads
    SELECT
        facebook_ads_basic_daily.ad_date AS advertisement_date          -- Дата показу реклами
        , 'Facebook Ads' AS media_source                                    -- Джерело реклами
        , facebook_campaign.campaign_name AS campaign_name                  -- Назва рекламної кампанії у Facebook
        , facebook_adset.adset_name AS advertisement_set_name               -- Назва набору оголошень у Facebook
        , facebook_ads_basic_daily.spend AS advertising_costs               -- Витрати на рекламу
        , facebook_ads_basic_daily.impressions AS advertisement_impressions -- Кількість показів реклами
        , facebook_ads_basic_daily.reach AS advertisement_reach             -- Охоплення реклами
        , facebook_ads_basic_daily.clicks AS advertisement_clicks           -- Кількість кліків
        , facebook_ads_basic_daily.leads AS advertisement_leads             -- Кількість отриманих лідів
        , facebook_ads_basic_daily.value AS advertisement_conversion_value  -- Загальний дохід від конверсій
    FROM facebook_ads_basic_daily
    LEFT JOIN facebook_adset 
        ON facebook_ads_basic_daily.adset_id = facebook_adset.adset_id           -- Об'єднання Facebook Ads з таблицею наборів оголошень
    LEFT JOIN facebook_campaign 
        ON facebook_ads_basic_daily.campaign_id = facebook_campaign.campaign_id  -- Об'єднання Facebook Ads з таблицею кампаній
),
combined_ads_data AS (
    -- Об'єднання даних з Facebook Ads і Google Ads у загальну таблицю
    SELECT * FROM facebook_ads_data
    UNION ALL
    SELECT
        google_ads_basic_daily.ad_date AS advertisement_date               -- Дата показу реклами
        , 'Google Ads' AS media_source                                     -- Джерело реклами
        , google_ads_basic_daily.campaign_name AS campaign_name            -- Назва рекламної кампанії у Google Ads
        , google_ads_basic_daily.adset_name AS advertisement_set_name      -- Назва набору оголошень у Google Ads
        , google_ads_basic_daily.spend AS advertising_costs                -- Витрати на рекламу
        , google_ads_basic_daily.impressions AS advertisement_impressions  -- Кількість показів реклами
        , NULL AS advertisement_reach                                      -- Охоплення реклами 
        , google_ads_basic_daily.clicks AS advertisement_clicks            -- Кількість кліків
        , google_ads_basic_daily.leads AS advertisement_leads              -- Кількість отриманих лідів
        , google_ads_basic_daily.value AS advertisement_conversion_value   -- Загальний дохід від конверсій
    FROM google_ads_basic_daily
),
aggregated_advertising_data AS (
    -- Агрегування даних за датою, джерелом, назвою кампанії та набором оголошень
    SELECT
        advertisement_date
        , media_source
        , campaign_name
        , advertisement_set_name
        , SUM(advertising_costs) AS total_advertising_costs                            -- Загальні витрати на рекламу
        , SUM(advertisement_impressions) AS total_advertisement_impressions            -- Загальна кількість показів
        , SUM(advertisement_clicks) AS total_advertisement_clicks                      -- Загальна кількість кліків
        , SUM(advertisement_conversion_value) AS total_advertisement_conversion_value  -- Загальний дохід від конверсій
    FROM combined_ads_data
    GROUP BY advertisement_date, media_source, campaign_name, advertisement_set_name
),
calculated_romi_for_campaigns AS (
    -- Обчислення ROMI (Return on Marketing Investment) для кожної кампанії
    SELECT
        campaign_name
        , SUM(total_advertising_costs) AS total_campaign_advertising_costs                  -- Загальні витрати на рекламу для кампанії
        , SUM(total_advertisement_conversion_value) AS total_campaign_conversion_value      -- Загальний дохід від конверсій для кампанії
        , SUM(total_advertisement_conversion_value) / NULLIF(SUM(total_advertising_costs), 0)
        AS return_on_marketing_investment                                                   -- ROMI = загальний дохід / загальні витрати
    FROM aggregated_advertising_data
    GROUP BY campaign_name
    HAVING SUM(total_advertising_costs) > 500000         -- Вибірка кампаній, де витрати перевищують 500 000
),
highest_romi_campaign AS (
    -- Вибірка кампанії з найвищим ROMI
    SELECT
        campaign_name
        , return_on_marketing_investment
    FROM calculated_romi_for_campaigns
    ORDER BY return_on_marketing_investment DESC
    LIMIT 1
),
highest_romi_advertisement_set AS (
    -- Вибірка набору оголошень (adset) з найвищим ROMI у кампанії з найвищим ROMI
    SELECT
        aggregated_advertising_data.campaign_name
        , aggregated_advertising_data.advertisement_set_name
        , SUM(aggregated_advertising_data.total_advertising_costs) AS total_advertising_costs_for_set
        , SUM(aggregated_advertising_data.total_advertisement_conversion_value) AS total_conversion_value_for_set
        , ROUND(SUM(aggregated_advertising_data.total_advertisement_conversion_value)
        / NULLIF(SUM(aggregated_advertising_data.total_advertising_costs), 0),2) AS return_on_marketing_investment_for_set
    FROM aggregated_advertising_data
    JOIN highest_romi_campaign 
        ON aggregated_advertising_data.campaign_name = highest_romi_campaign.campaign_name
    GROUP BY aggregated_advertising_data.campaign_name, aggregated_advertising_data.advertisement_set_name
    ORDER BY return_on_marketing_investment_for_set DESC
    LIMIT 1
)
-- Фінальний запит для отримання кампанії з найвищим ROMI та її найефективнішого набору оголошень
SELECT
    highest_romi_campaign.campaign_name AS campaign_with_highest_romi            -- Назва кампанії з найвищим ROMI
    , highest_romi_advertisement_set.advertisement_set_name 
    AS best_performing_advertisement_set                                         -- Найефективніший набір оголошень у кампанії
    , highest_romi_advertisement_set.total_advertising_costs_for_set 
    AS total_advertising_costs_of_set                                            -- Витрати цього набору оголошень
    , highest_romi_advertisement_set.total_conversion_value_for_set 
    AS total_conversion_value_of_set                                             -- Загальний дохід від конверсій
    , highest_romi_advertisement_set.return_on_marketing_investment_for_set 
    AS return_on_marketing_investment_of_set                                     -- ROMI для цього набору оголошень
FROM highest_romi_advertisement_set
JOIN highest_romi_campaign 
    ON highest_romi_advertisement_set.campaign_name = highest_romi_campaign.campaign_name;
