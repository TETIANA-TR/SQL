-- Крок 1. Створюємо CTE, щоб зібрати інформацію про залученість та активність користувача під час сесії.
WITH Engagement AS (
  SELECT
    user_pseudo_id   --ідентифікатор користувача.
    -- Отримуємо session_id із параметрів події.
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
    
    -- Перевіряємо, чи був користувач залучений (1 = так, 0 = ні)
    , MAX(IF(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1'
      , 1  --користувач був залучений
      , 0  --користувач не був залучений
    )) AS is_engaged
    
    -- Сума часу активності під час сесії (в мілісекундах)
    , SUM(
    IFNULL((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'), 0)) AS total_engagement_time_msec

  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'  --фільтруємо дані за 2020 рік.
  GROUP BY  --групуємо результати за ідентифікатором користувача та ідентифікатором сесії, щоб агрегувати дані на рівні сесії
    user_pseudo_id, session_id
)

-- Крок 2. Створюємо CTE з інформацією про покупки
, Purchases AS (
  SELECT
    DISTINCT user_pseudo_id  --унікальний ідентифікатор користувача.
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id --отримуємо ідентифікатор сесії для подій покупки
    , 1 AS made_purchase  --позначаємо, що в цій сесії була покупка
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'  --фільтруємо дані за 2020 рік.
    AND event_name = 'purchase'  --вибираємо тільки події покупки
)

-- Крок 3. Об’єднуємо інформацію про залученість та покупки, і рахуємо коефіцієнти кореляції.
SELECT
  -- Кореляція між фактом залученості та покупкою
  CORR(e.is_engaged, IF(p.made_purchase IS NULL, 0, 1)) AS corr_engagement_purchase
  
  -- Кореляція між тривалістю залучення та покупкою
  , CORR(e.total_engagement_time_msec, IF(p.made_purchase IS NULL, 0, 1)) AS corr_time_purchase
FROM
  Engagement e  --використовуємо агреговані дані про залученість на рівні сесії.
LEFT JOIN  --приєднуємо інформацію про сесії, в яких були покупки. Використовуємо LEFT JOIN, щоб зберегти всі сесії з таблиці Engagement.
  Purchases p  
ON
  e.user_pseudo_id = p.user_pseudo_id AND e.session_id = p.session_id;  --з'єднуємо таблиці за ідентифікатором користувача та ідентифікатором сесії, щоб співставити залученість з покупками в межах однієї сесії.
