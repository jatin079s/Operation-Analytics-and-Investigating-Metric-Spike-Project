create database jobs;
use jobs;

CREATE TABLE job_data (
    job_id INT,
    actor_id BIGINT,
    `event` VARCHAR(50),
    `language` VARCHAR(50),
    time_spent INT,
    org VARCHAR(50),
    ds DATE
);

select * from job_data;

-- Case Study 1 (Job Data)

/* A. Number of jobs reviewed: Amount of jobs reviewed over time.
Your task: Calculate the number of jobs reviewed per hour per day for November 2020? */

SELECT 
    ds,
    (COUNT(job_id) / SUM(time_spent) * 3600) AS jobs_per_hour_per_day
FROM
    job_data
WHERE
    ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
ORDER BY ds;


/* B. Throughput: It is the no. of events happening per second.
Your task: Let’s say the above metric is called throughput. 
Calculate 7 day rolling average of throughput? For throughput, do you prefer daily metric or 7-day rolling and why? */

-- Daily Throghput

SELECT 
    ds, num_of_events / total_time_spent AS daily_throughput
FROM
    (SELECT 
        ds,
            COUNT(event) AS num_of_events,
            SUM(time_spent) AS total_time_spent
    FROM
        job_data
    GROUP BY ds) j
GROUP BY ds;

-- Weekly Throghput (7 Day Rolling)

SELECT 
    num_of_events / total_time_spent AS weekly_throughput
FROM
    (SELECT 
        COUNT(event) AS num_of_events,
            SUM(time_spent) AS total_time_spent
    FROM
        job_data) j;


/* C. Percentage share of each language: Share of each language for different contents.
Your task: Calculate the percentage share of each language in the last 30 days? */

SELECT 
    language,
    ROUND((COUNT(language) * 100 / (SELECT 
                    COUNT(*)
                FROM
                    job_data)),
            2) AS percentage_share
FROM
    job_data
GROUP BY language;

/* D. Duplicate rows: Rows that have the same value present in them.
Your task: Let’s say you see some duplicate rows in the data. How will you display duplicates from the table? */

SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id) AS row_no
    FROM job_data
) AS a
WHERE row_no > 1;

-- Case Study 2 (Investigating metric spike)

CREATE TABLE users (
    user_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    company_id INT,
    `language` VARCHAR(50),
    activated_at TIMESTAMP DEFAULT NULL,
    state VARCHAR(50)
);

CREATE TABLE `events` (
    user_id INT,
    occurred_at TIMESTAMP,
    event_type VARCHAR(50),
    event_name VARCHAR(50),
    location VARCHAR(50),
    device VARCHAR(50),
    user_type INT
);

CREATE TABLE email_events (
    user_id INT,
    occurred_at TIMESTAMP,
    `action` VARCHAR(50),
    user_type INT
);


/* A. User Engagement: To measure the activeness of a user. Measuring if the user finds quality in a product/service.
Your task: Calculate the weekly user engagement? */

SELECT 
    COUNT(DISTINCT user_id) AS active_user,
    WEEK(occurred_at) AS week_num
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY week_num;


 /* B. User Growth: Amount of users growing over time for a product.
Your task: Calculate the user growth for product? */

select 
	year, week, num_users, 
	sum(num_users) over ( rows between unbounded preceding and current row) as active_users 
from 
	(select year(activated_at) as year, week(activated_at) as week, count(user_id) as num_users from users  
where 
	state='active'
group by year, week
order by year, week) as a; 

/* C. Weekly Retention: Users getting retained weekly after signing-up for a product.
Your task: Calculate the weekly retention of users-sign up cohort? */

SELECT 
    YEAR(OCCURRED_AT) AS YEAR,
    WEEK(OCCURRED_AT) AS WEEK,
    DEVICE,
    COUNT(DISTINCT user_id) AS users_retention
FROM
    events
WHERE
    EVENT_TYPE = 'ENGAGEMENT'
GROUP BY year , week , device
ORDER BY year , week , device;

/* D. Weekly Engagement: To measure the activeness of a user. Measuring if the user finds quality in a product/service weekly.
Your task: Calculate the weekly engagement per device? */

SELECT DISTINCT
    device,
    WEEK(occurred_at) AS week,
    COUNT(DISTINCT user_id) AS users_engagement
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY week , device
ORDER BY week , device;

/* E. Email Engagement: Users engaging with the email service.
Your task: Calculate the email engagement metrics? */

SELECT 
    YEAR(occurred_at) AS year,
    MONTH(occurred_at) AS month,
    action,
    COUNT(action) AS email_engagement
FROM
    email_events
GROUP BY year , month , action
ORDER BY year , month , action;




