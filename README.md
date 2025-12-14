**Fraudulent Transaction Analysis | SQL & Data Analytics Project**

In this project, I performed an end-to-end analysis of a financial transactions dataset to identify fraud patterns, customer risk levels, and transaction behavior trends. The goal was to clean the data, extract meaningful insights, and build KPIs useful for fraud detection.

** Key Steps Performed**

Data cleaning and preprocessing using SQL

Converted raw datetime fields into clean DATE and TIME columns

Created KPIs such as total transactions, fraud percentage, fraud amount & total amount ,average amount 

Performed monthly, weekly, and hourly transaction trend analysis

Identified top risky customers based on fraud ratio

Executed MoM % change analysis for customers, fraud amounts, and total transactions

Calculated moving averages of transaction amounts for behavior monitoring

** Insights & Analysis**

Calculated fraud percentage, total amount lost, and top 5 high-risk customers

Identified peak fraud hours, fraud-prone devices, and locations with maximum losses

Analyzed transaction trends by weekdays, months, and transaction type

Categorized customers into High / Medium / Low Risk using fraud frequency

Built trend analysis using window functions like LAG(), RANK(), and MOVING AVERAGE of 7 days .

**SQL Concepts Used**

Aggregate Functions(sum,count,avg)

CASE Statements

Date & Time Functions(month(),year(),monthname(),hour()

Window Functions: LAG(), RANK(), AVG() OVER()

CTEs (Common Table Expressions)

Grouping and Ordering

This project helped me strengthen my SQL expertise and understand real-world fraud detection

-- data cleaning 
describe fraud_Transaction;

update fraud_Transaction set transactiontime=str_to_date(transactiontime,'%d-%m-%Y %H:%i:%s');

**SQL Queries**


alter table fraud_data
modify column transactiontime date;

add two columns in the dataset
alter table fraud_transaction
add column Transaction_time time,
add column Transaction_date date;

update value to new columns 
update fraud_transaction set transaction_date=str_to_date(TransactionTime,'%d-%m-%Y %H:%i:%s'),
transaction_time=str_to_date(TransactionTime,'%d-%m-%Y %H:%i:%s');

-- KPI
-- TotalTarnsactions
select count(*) as totalTransaction from fraud_transaction;

 

-- total fraud 
select count(customerid) as fraudTransaction from fraud_transaction
where fraudflag=1;

 

-- total transaction amount
select round(sum(amount),0) as totalamount from fraud_transaction;

 

-- fraud_transaction
select round(sum(amount),0) as fraudamount from fraud_transaction
where fraudflag=1;

 
-- total customer 
select count(distinct customerid) as totalcustomer  from fraud_transaction;

 

-- fraud percentage
select concat(round(count(case when fraudflag=1 then 1 end) *100/count(*),2),'%') as fraud_percentage
from fraud_transaction;

 


-- monthly transaction trend
SELECT 
    MONTHNAME(transaction_date) AS monthName,
    ROUND(SUM(amount), 1) AS totalamount,
    ROUND(SUM(CASE
                WHEN fraudflag = 1 THEN amount
                ELSE 0
            END),
            1) AS fraud_Amount
FROM
    fraud_transaction
GROUP BY MONTHNAME(transaction_date)
ORDER BY MONTHNAME(transaction_date);

 

-- calculate Average transaction by location
SELECT 
    location, ROUND(AVG(amount), 2) AS avg_amount
FROM
    fraud_transaction
GROUP BY location;

 

-- calculate total amount,total amount lost due to fraud per location
SELECT 
    location,
    round(sum(Amount),2) as TotalAmount,
  round(sum(CASE
        WHEN fraudflag = 1 THEN amount else 0
    END),2) AS fraudAmount
FROM
    fraud_transaction
GROUP BY location;

 

-- fraud  by device
SELECT 
    device,
    COUNT(*) AS TotalTransaction,
	COUNT(CASE
        WHEN fraudflag = 1 THEN 1
    END) AS fraudcount
FROM
    fraud_transaction
GROUP BY device
ORDER BY fraudcount DESC;

 

-- fraud by transaction_type
SELECT 
    TransactionType,
    COUNT(*) AS TotalTransaction,
    COUNT(CASE
        WHEN fraudflag = 1 THEN 1
    END) AS fraudcount,
    round(sum(amount),1) as TotalAmount,
    round(sum(case when fraudflag = 1 then amount else 0 end),1) as fraudAmount
FROM
    fraud_transaction
GROUP BY TransactionType
ORDER BY fraudcount DESC;

 

-- Find Peak Transaction hours
SELECT 
    HOUR(transaction_time) AS transaction_hour,
    COUNT(*) AS TotalTransaction,
    count(case when  fraudflag = 1 then 1 end) as FraudCount,
    round(sum(amount),1) as totalamount,
    round(sum(case when fraudflag = 1 then amount else 0 end),1) as fraudAmount
FROM
    fraud_transaction
GROUP BY transaction_hour
ORDER BY FraudCount desc;

 

-- MOM % and difference Total Transaction
with mom_transaction as (
SELECT 
    MONTH(Transaction_date) AS month, COUNT(1) AS transaction_count
FROM
    fraud_transaction
GROUP BY MONTH(Transaction_date)
ORDER BY MONTH(Transaction_date)
)
select month, transaction_count,
transaction_count-lag(transaction_count) over (order by month) as Difference,
round(((transaction_count-lag(transaction_count) over (order by month))/
lag(transaction_count) over (order by month))*100,1) as percentage
from mom_transaction;


 

-- MOM %  totalCustomerr
with mom_transaction as (
SELECT 
    MONTH(Transaction_date) AS month,
    COUNT(DISTINCT customerid) AS customercount
FROM
    fraud_transaction
GROUP BY MONTH(Transaction_date)
ORDER BY MONTH(Transaction_date)
)
select month, customercount,
lag(customercount) over (order by month) as difference,
round(((customercount-lag(customercount) over (order by month))/lag(customercount) over (order by month))*100,1) as percentage
from mom_transaction;


 


-- MOM % and difference totaFraudlAmount
with mom_transaction as 
(
	SELECT 
		MONTH(Transaction_date) AS month,
		ROUND(SUM(CASE
					WHEN fraudflag = 1 THEN amount
					ELSE 0
				END),
				1) AS totalFraudamount
	FROM
		fraud_transaction
	GROUP BY MONTH(Transaction_date)
	ORDER BY MONTH(Transaction_date)
)
select month, totalFraudamount,
round(lag(totalFraudamount) over (order by month),1)as prev_month_Fraud_amount,
round(totalFraudamount-lag(totalFraudamount) over (order by month),1) as Difference,
round(((totalFraudamount-lag(totalFraudamount) over (order by month))/lag(totalFraudamount) over (order by month))*100,1) as percentage
from mom_transaction;

 

-- MOM % and difference totalAmount
with mom_transaction as (
	SELECT 
		MONTH(Transaction_date) AS month,
		ROUND(SUM(amount), 0) AS totalamount
	FROM
		fraud_transaction
	GROUP BY MONTH(Transaction_date)
	ORDER BY MONTH(Transaction_date)
)
select month, totalamount,
lag(totalamount) over (order by month) as prev_month_amount,
totalamount-lag(totalamount) over (order by month) as difference,
round(((totalamount-lag(totalamount) over (order by month))/lag(totalamount) over (order by month))*100,1) as percentage
from mom_transaction;

 

-- Calculate number of transaction and total amount by week
SELECT 
    DAYNAME(transaction_date) AS day_of_week,
    COUNT(*) AS totaltransaction,
    round(SUM(amount),1) AS totalAmount,
    round(SUM(CASE
        WHEN fraudflag = 1 THEN amount
        ELSE 0
    END),1) AS FraudAmount,
    CONCAT(ROUND(COUNT(CASE
                        WHEN fraudflag = 1 THEN 1
                    END) * 100 / COUNT(*),
                    2),
            '%') AS fraud_percentage
FROM
    fraud_transaction
GROUP BY DAYNAME(transaction_date)
ORDER BY fraud_percentage desc;


 

-- top 5 customer with higest fraud amount
 with higestfraudamount as (
	 SELECT 
		customerid,
		SUM(CASE
			WHEN FraudFlag = 1 THEN amount
			ELSE 0
		END) AS fraud_amount
	FROM
		fraud_transaction
	GROUP BY customerid
 ),
 rank_fraud as 
 (
   select *,
   rank()over(order by fraud_amount desc) as rnk
   from  higestfraudamount
 )
 select * from rank_fraud
 where rnk<=5
 
 
 SELECT 
    customerid, SUM(amount)
FROM
    fraud_transaction
WHERE
    FraudFlag = 1
GROUP BY customerid
ORDER BY SUM(amount) DESC
LIMIT 5;

 



-- fraud trend by customer 
with fraud_trend as (
SELECT 
    customerid,
    COUNT(1) AS totaltransaction,
        COUNT(CASE
        WHEN fraudflag = 1 THEN 1
    END) AS fraud_transaction_per_customer,
    sum(amount) as totalAmount,
    sum(case when fraudflag=1 then amount end) as fraud_amount_per_customer
FROM
    fraud_transaction
	GROUP BY customerid)
SELECT 
    customerid,
    totaltransaction,
    fraud_transaction_per_customer,
    fraud_amount_per_customer,
    ROUND((fraud_transaction_per_customer / totaltransaction) * 100,
            2) AS fraud_percentage,
    CASE
        WHEN
            ROUND((fraud_transaction_per_customer / totaltransaction) * 100,
                    2) > 40
        THEN
            'High Risk'
        WHEN
            ROUND((fraud_transaction_per_customer / totaltransaction) * 100,
                    2) > 20
        THEN
            'medium Risk'
        ELSE 'Low Risk'
    END AS risk_status
FROM
    fraud_trend
ORDER BY fraud_transaction_per_customer DESC;


 


-- Calculate the moving average of Transaction Amount for each customer .
  SELECT 
    customerid, transaction_date, amount,
    avg(amount)over(partition by customerid order by transaction_date
	rows between 6 preceding and current row) as moving_avg
    
FROM
    fraud_transaction;

 


