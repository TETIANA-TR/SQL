-- Крок 1. Вибираємо інформацію про початок сесії, включаючи дату, користувача, сесію та шлях сторінки (page_path)
WITH SessionStarts AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date --перетворюємо мікросекунди в дату
    , user_pseudo_id  --ідентифікатор користувача
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id --ідентифікатор сесії (дістається з event_params)

    --виділяємо шлях сторінки (page_path) із повної URL (page_location), без домену та параметрів
    , REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') 
      , r'^(?:https?://[^/]+)?([^?#]+)'
    ) AS page_path
  FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
   _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'   --обмежуємо діапазон дат (2020 рік)
    AND event_name = 'session_start'  --вибираємо тільки події початку сесії
),

-- Крок 2. Вибираємо інформацію про покупки (event_name = 'purchase'), разом з user_id та session_id
Purchases AS (
  SELECT
    user_pseudo_id     --ідентифікатор користувача
    , (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id  --ідентифікатор сесії
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'  --обмежуємо аналіз даними за 2020 рік.
    AND event_name = 'purchase'  --фільтруємо, залишаючи лише події покупки.
)

-- Крок 3. Підсумковий запит: об'єднуємо дані про початок сесій та покупки, групуємо за шляхом сторінки, рахуємо кількість сесій, кількість покупок та розраховуємо коефіцієнт конверсії.
SELECT
  s.page_path  --шлях сторінки початку сесії.
  , COUNT(DISTINCT CONCAT(s.user_pseudo_id, '-', s.session_id)) AS user_sessions_count --рахуємо кількість унікальних сесій.
  , COUNT(DISTINCT CONCAT(p.user_pseudo_id, '-', p.session_id)) AS purchase_count  --рахуємо кількість унікальних покупок.
  , SAFE_DIVIDE(     --обчислюємо конверсію: пповертає NULL, якщо знаменник дорівнює нулю, запобігаючи помилкам ділення.
    COUNT(DISTINCT CONCAT(p.user_pseudo_id, '-', p.session_id))
    , COUNT(DISTINCT CONCAT(s.user_pseudo_id, '-', s.session_id))
  ) AS conversion_rate  --коефіцієнт конверсії від початку сесії на сторінці до здійснення покупки.
FROM
  SessionStarts s   --використовуємо дані про початок сесій.
LEFT JOIN   --з’єднуємо по user_id та session_id
  Purchases p
 ON s.user_pseudo_id = p.user_pseudo_id AND s.session_id = p.session_id
GROUP BY  --групуємо результати за шляхом сторінки початку сесії, щоб агрегувати метрики для кожної унікальної сторінки.
  s.page_path
ORDER BY   --сортуємо за конверсією у спадному порядку
  conversion_rate DESC;