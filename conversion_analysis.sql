WITH SessionStarts AS (
  --Крок 1: Вибираємо дані про початок сесій (session_start)
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date  --перетворюємо час події в дату
    , user_pseudo_id   --ідентифікатор користувача
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id --отримуємо ID сесії
    , traffic_source.source AS source  --джерело трафіку
    , traffic_source.medium AS medium  --канал трафіку
    , traffic_source.name AS campaign  --назва рекламної кампанії
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE 
   _TABLE_SUFFIX BETWEEN '20210101' AND '20211231' -- фільтруємо дані за 2021 рік
    AND event_name = 'session_start'  --вибираємо тільки події початку сесії
)
, CartAdditions AS (
  -- Крок 2: Вибираємо дані про додавання товарів до кошика (add_to_cart)
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date  --перетворюємо час події в дату
    , user_pseudo_id   --ідентифікатор користувача
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id  --отримуємо ID сесії
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231' --фільтруємо дані за 2021 рік
    AND event_name = 'add_to_cart'  --вибираємо тільки події додавання до кошика
)
, Checkouts AS (
  -- Крок 3: Вибираємо дані про початок оформлення замовлення (begin_checkout)
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date  --перетворюємо час події в дату
    , user_pseudo_id   --ідентифікатор користувача
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id  --отримуємо ID сесії
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'  --фільтруємо дані за 2021 рік
    AND event_name = 'begin_checkout'  --вибираємо тільки події початку оформлення
)
, Purchases AS (
  -- Крок 4: Вибираємо дані про покупки (purchase)
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date  --перетворюємо час події в дату
    , user_pseudo_id  --ідентифікатор користувача
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id  --отримуємо ID сесії
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'   --фільтруємо дані за 2021 рік
    AND event_name = 'purchase'    --вибираємо тільки події покупки
)
SELECT
  s.event_date    --дата початку сесії
  , s.source      --джерело трафіку початку сесії
  , s.medium      --канал трафіку початку сесії
  , s.campaign    --рекламна кампанія трафіку початку сесії
  , COUNT(DISTINCT CONCAT(s.user_pseudo_id, '-', s.session_id)) AS user_sessions_count   --кількість унікальних сесій
  , SAFE_DIVIDE(
    COUNT(DISTINCT CONCAT(c.user_pseudo_id, '-', c.session_id))
    , COUNT(DISTINCT CONCAT(s.user_pseudo_id, '-', s.session_id))
) AS visit_to_cart  --конверсія до кошика
  , SAFE_DIVIDE(
    COUNT(DISTINCT CONCAT(ch.user_pseudo_id, '-', ch.session_id))
    , COUNT(DISTINCT CONCAT(s.user_pseudo_id, '-', s.session_id))
) AS visit_to_checkout  --конверсія до оформлення
  , SAFE_DIVIDE(
    COUNT(DISTINCT CONCAT(p.user_pseudo_id, '-', p.session_id))
    , COUNT(DISTINCT CONCAT(s.user_pseudo_id, '-', s.session_id))
) AS visit_to_purchase  --конверсія до покупки
FROM
  SessionStarts s  --беремо дані про початок сесій
LEFT JOIN  --приєднуємо дані про додавання до кошика за датою, користувачем та сесією
  CartAdditions c ON s.event_date = c.event_date AND s.user_pseudo_id = c.user_pseudo_id AND s.session_id = c.session_id
LEFT JOIN  --приєднуємо дані про початок оформлення за датою, користувачем та сесією
  Checkouts ch ON s.event_date = ch.event_date AND s.user_pseudo_id = ch.user_pseudo_id AND s.session_id = ch.session_id  
LEFT JOIN  --приєднуємо дані про покупки за датою, користувачем та сесією
  Purchases p ON s.event_date = p.event_date AND s.user_pseudo_id = p.user_pseudo_id AND s.session_id = p.session_id  
GROUP BY  --групуємо результати за датою та каналами трафіку
  s.event_date
  , s.source
  , s.medium
  , s.campaign
ORDER BY  --сортуємо результати за датою
  s.event_date;