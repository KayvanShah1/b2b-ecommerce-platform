-- Report Question:
-- What are the most popular used devices for B2B clients (top 5)?
--
-- Intuition:
-- We classify user agents into coarse device families and rank by request volume.

with classified_web_events as (
    select
        case
            when user_agent is null or trim(user_agent) = '' or user_agent = '-' then 'Unknown'
            when lower(user_agent) like '%bot%' or lower(user_agent) like '%crawler%' or lower(user_agent) like '%spider%'
                then 'Bot/Crawler'
            when lower(user_agent) like '%iphone%' then 'iPhone'
            when lower(user_agent) like '%android%' then 'Android'
            when lower(user_agent) like '%ipad%' or lower(user_agent) like '%tablet%' then 'Tablet'
            when lower(user_agent) like '%windows%' then 'Windows Desktop'
            when lower(user_agent) like '%macintosh%' or lower(user_agent) like '%mac os%' then 'Mac Desktop'
            when lower(user_agent) like '%linux%' then 'Linux Desktop'
            else 'Other'
        end as device_family,
        username
    from {{ ref('fct_webserver_logs') }}
    where username is not null
      and username <> '-'
),
ranked as (
    select
        device_family,
        count(*) as request_count,
        count(distinct username) as unique_users
    from classified_web_events
    group by 1
)
select
    device_family,
    request_count,
    unique_users
from ranked
order by request_count desc, unique_users desc, device_family
limit 5
