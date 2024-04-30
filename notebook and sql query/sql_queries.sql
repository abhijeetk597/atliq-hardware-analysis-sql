use gdb0041;
-- Data Exploration (also performed using Python Pandas)get_top_n_products_by_net_sales
-- no of customers -> 74
SELECT COUNT(DISTINCT customer) FROM dim_customer;

SELECT * FROM dim_customer;
-- what platforms are there -> 2 -> Brick & Mortar / E-Commerce
SELECT DISTINCT platform FROM dim_customer;

-- in which countries atliq products are sold? -> 27 different countries
SELECT DISTINCT market FROM dim_customer;

-- what are subzones -> 7
SELECT DISTINCT sub_zone FROM dim_customer;

/* Task 1:
Generate a report of individual product sales (aggregated on a monthly basis at the product code level) for Chroma India customer for FY=2021.
The report should have the following fields.
1. Month
2. Product Name
3. Variant
4. Sold Quantity
5. Gross Price per Item
6. Gross Price total */

-- create a user defined function to get fiscal year
/*
CREATE FUNCTION `get_fiscal_year` (calendar_date DATE)
	RETURNS INTEGER
DETERMINISTIC
BEGIN
	DECLARE fiscal_year INT;
    SET fiscal_year = YEAR(DATE_ADD(calendar_date, INTERVAL 4 MONTH));
	RETURN fiscal_year;
END
*/
-- create a user defined function to get fiscal quarter
/*
CREATE FUNCTION `get_fiscal_quarter`(calendar_date DATE)
RETURNS CHAR(2)
    DETERMINISTIC
BEGIN
	DECLARE m TINYINT;
    DECLARE qtr CHAR(2);
    SET m = MONTH(calendar_date);
    CASE
		WHEN m IN (9, 10, 11) THEN
			SET qtr = "Q1";
		WHEN m IN (12, 1, 2) THEN
			SET qtr = "Q2";
		WHEN m IN (3, 4, 5) THEN
			SET qtr = "Q3";
		WHEN m IN (6, 7, 8) THEN
			SET qtr = "Q4";
	END CASE; 
RETURN qtr;
END
*/

-- filter chroma india from fact_sales_monthly
WITH cte AS(
    SELECT customer_code 
    FROM dim_customer
    WHERE customer LIKE '%croma%' AND market LIKE '%india%'
)
SELECT 
	MONTH(s.date) AS month, p.product, p.variant, s.sold_quantity, 
    ROUND(g.gross_price, 2) AS gross_price, 
    ROUND(s.sold_quantity*g.gross_price, 2) AS gross_price_total
FROM fact_sales_monthly s JOIN dim_product p
ON s.product_code = p.product_code
JOIN fact_gross_price g
ON 	g.product_code = s.product_code AND
	g.fiscal_year = get_fiscal_year(s.date)
WHERE
	customer_code = (SELECT * FROM cte) AND
    get_fiscal_year(date) = 2021
ORDER BY date ASC;
    
/* Task 2:
Create aggregated monthly gross sales report for Croma India customer.
The report should have following fields.
1. Month
2. Total gross sales amount to chroma india in that month */
WITH cte AS(
    SELECT customer_code 
    FROM dim_customer
    WHERE customer LIKE '%croma%' AND market LIKE '%india%'
)
SELECT 
	DATE_FORMAT(s.date, '%m-%Y') AS month, 
    SUM(s.sold_quantity*g.gross_price) AS gross_price_total
FROM fact_sales_monthly s JOIN fact_gross_price g
ON 	g.product_code = s.product_code AND
	g.fiscal_year = get_fiscal_year(s.date)
WHERE
	customer_code = (SELECT * FROM cte)
GROUP BY 1;

/* Task 3:
Generate a yearly report for Croma India where there are two columns
1. Fiscal Year
2. Total Gross Sales amount In that year from Croma
*/
WITH cte AS(
    SELECT customer_code 
    FROM dim_customer
    WHERE customer LIKE '%croma%' AND market LIKE '%india%'
)
SELECT 
	get_fiscal_year(s.date) AS fiscal_year, 
    SUM(s.sold_quantity*g.gross_price) AS gross_price_total
FROM fact_sales_monthly s JOIN fact_gross_price g
ON 	g.product_code = s.product_code AND
	g.fiscal_year = get_fiscal_year(s.date)
WHERE
	customer_code = (SELECT * FROM cte)
GROUP BY 1;

/*Task 4:
Create a stored procedure to get monthly gross sales report for any customer

CREATE PROCEDURE `get_monthly_gross_sales_for_customer` (c_codes TEXT)
BEGIN
	SELECT 
		DATE_FORMAT(s.date, '%m-%Y') AS month, 
		ROUND(SUM(s.sold_quantity*g.gross_price),2) AS gross_price_total
	FROM fact_sales_monthly s JOIN fact_gross_price g
	ON 	g.product_code = s.product_code AND
		g.fiscal_year = get_fiscal_year(s.date)
	WHERE
		FIND_IN_SET(s.customer_code, c_codes)
	GROUP BY 1;
END
*/

/*Task 5:
Create a stored procedure that can determine the market badge based on the following logic. 
If *total_sold_quantity > 5 million* that market is considered *Gold* else it is *Silver*

Input to the stored proc will be:
- markte
- fiscal_year

Output
- market_badge

CREATE PROCEDURE `get_market_badge`(
	IN in_market VARCHAR(45),
    IN in_fiscal_year YEAR,
    OUT out_badge VARCHAR(20)
)
BEGIN
	DECLARE qty INT DEFAULT 0;
    
    # set default market to be india
    IF in_market = "" THEN
		SET in_market = "india";
	END IF;
    
    # retrieve total qty for a given market + fyear
    SELECT SUM(sold_quantity) INTO qty
    FROM fact_sales_monthly s
    JOIN dim_customer c
    ON s.customer_code = c.customer_code
    WHERE
		get_fiscal_year(s.date) = in_fiscal_year AND
        c.market = in_market
	GROUP BY c.market;
    
    # determine market badge
    IF qty > 5000000 THEN
		SET out_badge = "Gold";
	ELSE
		SET out_badge = "Silver";
	END IF;
END
*/

/* Task 6:
Create a `database view` with following columns.
- date
- product_code,
- product
- variant
- sold_quantity
- gross_price_per_item
- gross_price_total
- pre_invoice_discount_pct

-- database view #sales_preinv_discount
CREATE VIEW `sales_preinv_discount` AS
SELECT 
	s.date, s.product_code,
	p.product, p.variant,
	s.sold_quantity,
	g.gross_price AS gross_price_per_item,
	ROUND(s.sold_quantity * g.gross_price, 2) AS gross_price_total,
	pre.pre_invoice_discount_pct
FROM fact_sales_monthly s
JOIN dim_customer c
	ON s.customer_code = c.customer_code
JOIN dim_product p
	ON s.product_code = p.product_code
JOIN fact_gross_price g
	ON g.fiscal_year = s.fiscal_year
	AND g.product_code = s.product_code
JOIN fact_pre_invoice_deductions AS pre
	ON pre.customer_code = s.customer_code
	AND pre.fiscal_year = s.fiscal_year


-- database view #sales_postinv_discount    
CREATE VIEW sales_postinv_discount AS
SELECT 
    s.date, s.fiscal_year,
    s.customer_code, s.market,
    s.product_code, s.product,
    s.variant, s.sold_quantity,
    s.gross_price_total,
    s.pre_invoice_discount_pct,
    (s.gross_price_total - (s.pre_invoice_discount_pct * s.gross_price_total)) AS net_invoice_sales,
    (po.discounts_pct + po.other_deductions_pct) AS post_invoice_discount_pct
FROM
    sales_preinv_discount s
    JOIN fact_post_invoice_deductions po 
        ON po.customer_code = s.customer_code
        AND po.product_code = s.product_code
        AND po.date = s.date
    
-- database view # net_sales
CREATE VIEW `net_sales` AS
SELECT *,
	(1 - post_invoice_discount_pct)*net_invoice_sales as net_sales
FROM sales_postinv_discount;
*/

-- Task 7: Top 5 markets by net_sales for fy 2021
SELECT 
    market,
    ROUND(SUM(net_sales)/1000000, 2) AS net_sales_mln
FROM gdb0041.net_sales
WHERE fiscal_year = 2021
GROUP BY market
ORDER BY net_sales_mln DESC
LIMIT 5;

-- Task 8: Top 5 customers by net_sales for fy 2021
SELECT c.customer, ROUND(SUM(net_sales)/1000000, 2) AS net_sales_mln
FROM gdb0041.net_sales n JOIN dim_customer c
ON n.customer_code = c.customer_code
WHERE fiscal_year = 2021
GROUP BY c.customer
ORDER BY net_sales_mln DESC
LIMIT 5;

-- Task 9: Top 5 products by net_sales for fy 2021
SELECT n.product, ROUND(SUM(net_sales)/1000000, 2) AS net_sales_mln
FROM gdb0041.net_sales n
WHERE fiscal_year = 2021
GROUP BY 1
ORDER BY net_sales_mln DESC
LIMIT 5;

-- Task 10: Bar Chart report for fy 2021 for top 10 markets by % net sales
SELECT n.market, 
		ROUND(SUM(net_sales)/1000000, 2) AS net_sales_mln
FROM gdb0041.net_sales n
WHERE fiscal_year = 2021
GROUP BY 1
ORDER BY net_sales_mln DESC;

-- Task 11: Create region wise (APAC, EU, LTAM etc) % net sales breakdown 
-- by customers in a respective region so regional analysis can be performed.
WITH cte AS(
SELECT
	c.customer,
    c.region,
    ROUND(SUM(net_sales)/1000000, 2) AS net_sales_mln
FROM net_sales s JOIN dim_customer c
ON s.customer_code = c.customer_code
WHERE s.fiscal_year = 2021
GROUP BY 1, 2)
SELECT
	*,
    net_sales_mln*100/SUM(net_sales_mln) OVER(PARTITION BY region) AS pct_share_region
FROM cte
ORDER BY region, pct_share_region DESC;

-- Task 12: Retrieve the top 2 markets in every region by their gross sales amount in FY=2021
WITH cte1 AS(
	SELECT
		c.market,
		c.region,
		ROUND(SUM(gross_price_total)/1000000, 2) AS gross_sales_mln
	FROM net_sales s JOIN dim_customer c
	ON s.customer_code = c.customer_code
	WHERE s.fiscal_year = 2021
	GROUP BY 1, 2),
cte2 AS(
	SELECT
		*,
		RANK() OVER(PARTITION BY region ORDER BY gross_sales_mln DESC) rn
	FROM cte1)
SELECT *
FROM cte2
WHERE rn <= 2;



