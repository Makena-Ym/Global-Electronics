-- creating a database
Create  database GlobalElectronics_DB;
GO

USE GlobalElectronics_DB;
GO
------------------------------------
-- -----Create the layers-----------
------------------------------------
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO

---------------------------------------------------
------- Create the tables in the Bronze layer------
----------------------------------------------------

--PRODUCTS TABLE
CREATE TABLE Bronze.Products (
    ProductKey INT PRIMARY KEY,
    ProductName NVARCHAR(255),
    Brand NVARCHAR(255),
    Color NVARCHAR(50),
    UnitCostUSD DECIMAL(18,2),
    UnitPriceUSD DECIMAL(18,2),
    SubcategoryKey INT,
    Subcategory NVARCHAR(255),
    CategoryKey INT,
    Category NVARCHAR(255)
);

DROP TABLE IF EXISTS Bronze.Products;
GO

DROP TABLE IF EXISTS Bronze.Data_Dictionary;
GO


--CUSTOMERS TABLE
CREATE TABLE Bronze.Customers (
    CustomerKey INT PRIMARY KEY,
    Gender NVARCHAR(50),
    Name NVARCHAR(255),
    City NVARCHAR(255),
    StateCode NVARCHAR(50),
    State NVARCHAR(255),
    ZipCode NVARCHAR(50),
    Country NVARCHAR(100),
    Continent NVARCHAR(100),
    Birthday DATE
);

--SALES TABLE(FACT TABLE)
CREATE TABLE Bronze.Sales (
    OrderNumber INT,
    LineItem INT,
    OrderDate DATE,
    DeliveryDate DATE,
    CustomerKey INT,
    StoreKey INT,
    ProductKey INT,
    Quantity INT,
    CurrencyCode NVARCHAR(10)
);

--STORES TABLE
--this table tells us the physical location and size of each retail outlet.
CREATE TABLE Bronze.Stores (
    StoreKey INT PRIMARY KEY,
    Country NVARCHAR(100),
    State NVARCHAR(100),
    SquareMeters INT,
    OpenDate DATE
);
-- EXPLANATION: SquareMeters is vital for "Sales per Square Meter" analysis later.

--Exchange Rates Table
CREATE TABLE Bronze.ExchangeRates (
    CurrencyCode NVARCHAR(10),
    ExchangeDate DATE,
    ExchangeRate DECIMAL(18,4)
);

drop table if exists Bronze.ExchangeRates;

-- EXPLANATION: Using DECIMAL(18,4) because exchange rates need high precision 
-- (e.g., 0.9245). Using FLOAT would cause rounding errors in your revenue totals.



/* ==========================================================================
   BULK INSERT SECTION: BRONZE LAYER
   Objective: Ingest raw CSV data into SQL Server Tables.

   ==========================================================================
*/


-- 1. LOAD CUSTOMERS
BULK INSERT Bronze.Customers
FROM 'C:\Data\GlobalElectronics\Customers.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,           -- Skips the header row
    FIELDTERMINATOR = ',',  -- Standard CSV separator
    ROWTERMINATOR = '0x0a'  -- '0x0a' is a Line Feed (common in these datasets)
);
GO 

-- 2. LOAD SALES (Fact Table)
BULK INSERT Bronze.Sales
FROM 'C:\Data\GlobalElectronics\Sales.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a'
);
GO

-- 3. LOAD STORES
BULK INSERT Bronze.Stores
FROM 'C:\Data\GlobalElectronics\Stores.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a'
);
GO




/* ================================================================================
STRATEGIC INGESTION NOTE: PRODUCTS & DATA DICTIONARY
================================================================================
While the majority of this project utilizes scripted BULK INSERT for scalability, 
the 'Products','ExchangeRates' and 'Data Dictionary' tables were ingested via the SSMS Import 
Wizard for the following architectural reasons:

1. DATA SANITIZATION: These files contained complex string qualifiers and 
   nested delimiters (commas within text descriptions) that triggered 
   OLE DB provider interface exceptions (Error 7301).
   
2. SCHEMA FLEXIBILITY: The Wizard was used to bypass rigid OLE DB mapping 
   and enforce an 'NVARCHAR(MAX)' landing zone, ensuring 100% data capture 
   without row-level truncation or "Type Mismatch" failures.

3. EFFICIENCY: In a production environment, when metadata files or dimension 
   tables have non-standard encoding, a Senior Analyst pivots to more 
   forgiving drivers to maintain project velocity.

The logic below continues with the 'Silver Layer' transformations, where these 
flexible Bronze inputs are cleaned and cast into strict data types.
================================================================================
*/


----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
   --        SILVER LAYER TRANSFORMATIONS
----------------------------------------------------------------------------------
---------------------------------------------------------------------------------- 
--1 CREATE SILVER PRODUCTS TABLE

SELECT TOP 0 * FROM Bronze.Products;
-- Drop if exists so we can recreate it cleanly
DROP TABLE IF EXISTS Silver.Products;
GO

SELECT 
    CAST([ProductKey] AS INT) AS ProductKey,
    TRIM([Product_Name]) AS ProductName, -- Mapping Product_Name to ProductName
    TRIM([Brand]) AS Brand,
    TRIM([Color]) AS Color,
    -- Cleaning the prices using your specific column names (Unit_Cost_USD)
    CAST(REPLACE(REPLACE([Unit_Cost_USD], '$', ''), ',', '') AS DECIMAL(18,2)) AS UnitCostUSD,
    CAST(REPLACE(REPLACE([Unit_Price_USD], '$', ''), ',', '') AS DECIMAL(18,2)) AS UnitPriceUSD,
    CAST([SubcategoryKey] AS INT) AS SubcategoryKey,
    TRIM([Subcategory]) AS Subcategory,
    CAST([CategoryKey] AS INT) AS CategoryKey,
    TRIM([Category]) AS Category
INTO Silver.Products
FROM Bronze.Products;
GO

-- Verification: Check the first 5 rows to ensure prices are numbers
SELECT TOP 5 ProductName, UnitCostUSD, UnitPriceUSD 
FROM Silver.Products;

--CREATE SILVER CUSTOMERS TABLE
SELECT TOP 0 * FROM Bronze.Customers;-- THIS IS TO CHECK THE HEADER NAMES
DROP TABLE IF EXISTS Silver.Customers;
GO

SELECT 
    CAST(CustomerKey AS INT) AS CustomerKey,
    TRIM(Gender) AS Gender,
    TRIM(Name) AS Name,
    TRIM(City) AS City,
    UPPER(TRIM(StateCode)) AS StateCode,
    TRIM(State) AS State,
    TRIM(ZipCode) AS ZipCode,
    TRIM(Country) AS Country,
    TRIM(Continent) AS Continent,
    -- Standardizing the date format
    CAST(Birthday AS DATE) AS Birthday
INTO Silver.Customers
FROM Bronze.Customers;
GO
-- Verification: Check the first 5 rows to ensure dates are correct
SELECT TOP 5 Name, Birthday
FROM Silver.Customers;

-- CREATE SILVER SALES TABLE
SELECT 
    CAST(OrderNumber AS INT) AS OrderNumber,
    CAST(LineItem AS INT) AS LineItem,
    CAST(OrderDate AS DATE) AS OrderDate,
    -- Handling potential NULLs in Delivery Date (items not yet delivered)
    CAST(DeliveryDate AS DATE) AS DeliveryDate,
    -- Calculated Column: How long did shipping take?
    DATEDIFF(day, OrderDate, DeliveryDate) AS DeliveryDays,
    CAST(CustomerKey AS INT) AS CustomerKey,
    CAST(StoreKey AS INT) AS StoreKey,
    CAST(ProductKey AS INT) AS ProductKey,
    CAST(Quantity AS INT) AS Quantity,
    TRIM(CurrencyCode) AS CurrencyCode
INTO Silver.Sales
FROM Bronze.Sales
WHERE OrderDate IS NOT NULL; -- Quality check: ignore broken rows

-----------------------------------------------------------------
-------- CREATE SILVER STORES TABLE------------------------------
-----------------------------------------------------------------
SELECT 
    CAST(StoreKey AS INT) AS StoreKey,
    TRIM(Country) AS Country,
    TRIM(State) AS State,
    CAST(SquareMeters AS INT) AS SquareMeters,
    CAST(OpenDate AS DATE) AS OpenDate,
    -- Calculated Column: How many years has the store been open?
    DATEDIFF(year, OpenDate, GETDATE()) AS StoreAgeYears
INTO Silver.Stores
FROM Bronze.Stores;


------------------------------------------------------------------
-------- CREATE SILVER EXCHANGE RATES TABLE----------------------
------------------------------------------------------------------
SELECT 
    TRIM(Currency) AS CurrencyCode,
    CAST(Date AS DATE) AS ExchangeDate,
    CAST(Exchange AS DECIMAL(18,4)) AS ExchangeRate
INTO Silver.ExchangeRates
FROM Bronze.ExchangeRates;

SELECT TOP 5 * FROM Silver.ExchangeRates; -- Verify the exchange rates look correct

------------------------------------------------------------------------------------------
------------------BUSINESS QUESTIONS------------------------------------------------------
-----------------------------------------------------------------------------------------
--1. Customer Demographics: "Who are our customers?"
SELECT 
    Gender,
    COUNT(CustomerKey) AS TotalCustomers,
    AVG(DATEDIFF(YEAR, Birthday, GETDATE())) AS AverageAge
FROM Silver.Customers
GROUP BY Gender;

/* RESULTS:
Male	7748	57
Female	7518	57 
this shows that we have a fairly even gender distribution among our customers, with an average age of 57 years.
This insight can help us tailor our marketing strategies and product offerings to better meet the needs of our customer base.
*/

--2. Sales Performance: "Which product category makes the most profit?"
SELECT 
    p.Category,
    ROUND(SUM(s.Quantity * p.UnitPriceUSD), 2) AS TotalRevenue,
    ROUND(SUM(s.Quantity * (p.UnitPriceUSD - p.UnitCostUSD)), 2) AS TotalProfit,
    ROUND((SUM(s.Quantity * (p.UnitPriceUSD - p.UnitCostUSD)) / SUM(s.Quantity * p.UnitPriceUSD)) * 100, 2) AS ProfitMarginPct
FROM Silver.Sales s
JOIN Silver.Products p ON s.ProductKey = p.ProductKey
GROUP BY p.Category
ORDER BY TotalProfit DESC;

/* RESULTS:
Category	TotalRevenue	TotalProfit	ProfitMarginPct
Computers	19301595.46	11277447.90	58.430000
Home Appliances	10795478.59	6296338.85	58.320000
Cameras and camcorders	6520168.02	3919800.99	60.120000
TV and Video	5928982.69	3536694.39	59.650000
Cell phones	6183791.22	3498626.54	56.580000
Music, Movies and Audio Books	3131006.44	1909259.17	60.980000
Audio	3169627.74	1827851.77	57.670000
Games and Toys	724829.43	396668.77	54.730000

This analysis reveals that the "Computers" category generates the highest total revenue and profit,
with a profit margin of approximately 58.43%. This insight can guide inventory management and
promotional efforts to focus on high-performing categories.
*/

--3. Store Analysis: "Which store has been open the longest and how is it performing?"
SELECT TOP 5
    s.StoreKey,
    s.Country,
    s.StoreAgeYears,
    SUM(sl.Quantity) AS TotalItemsSold
FROM Silver.Stores s
JOIN Silver.Sales sl ON s.StoreKey = sl.StoreKey
GROUP BY s.StoreKey, s.Country, s.StoreAgeYears
ORDER BY s.StoreAgeYears DESC;

/* RESULTS:
StoreKey	Country	StoreAgeYears	TotalItemsSold
9	Canada	21	4894
37	United Kingdom	21	3028
17	France	19	626
45	United States	19	4672
1	Australia	18	871 
This analysis identifies the longest-standing stores and their sales performance. 
Store 9 in Canada has been open for 21 years and has sold 4,894 items, indicating strong performance over time. 
This information can be used to analyze factors contributing to the success of long-standing stores and apply those insights to newer locations.
*/

--4. Shipping Efficiency: "Are we getting faster at delivering products?"
SELECT 
    YEAR(OrderDate) AS OrderYear,
    AVG(DeliveryDays) AS AvgDeliveryTime
FROM Silver.Sales
WHERE DeliveryDays IS NOT NULL
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear;
/* RESULTS:
OrderYear	AvgDeliveryTime
2016	7
2017	5
2018	4
2019	4
2020	4
2021	3

this analysis shows a clear improvement in delivery times over the years, 
with the average delivery time decreasing from 7 days in 2016 to 3 days in 2021.
This trend suggests that the company has been successful in optimizing its logistics and supply chain processes,
leading to faster deliveries and potentially higher customer satisfaction.
*/

--5. Global Impact: "What is our revenue by Country?"
SELECT 
    c.Country,
    COUNT(s.OrderNumber) AS OrderCount,
    SUM(s.Quantity * p.UnitPriceUSD) AS TotalRevenueUSD
FROM Silver.Sales s
JOIN Silver.Customers c ON s.CustomerKey = c.CustomerKey
JOIN Silver.Products p ON s.ProductKey = p.ProductKey
GROUP BY c.Country
ORDER BY TotalRevenueUSD DESC;

/* RESULTS:
Country	OrderCount	TotalRevenueUSD
United States	33767	29871631.17
United Kingdom	8140	7084088.12
Germany	5956	5414149.80
Canada	5415	4724334.63
Australia	2941	2708137.61
Italy	2685	2475645.77
Netherlands	2250	1962154.27
France	1730	1515338.22

This analysis highlights the United States as the largest market in
terms of both order count and total revenue, 
followed by the United Kingdom and Germany.
This information can inform market-specific strategies and resource allocation to maximize growth in key regions.
*/

-----------------------------------------------------------------------------------------------------------------
----------------------GOLD LAYER PREPARATION--------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

--. The Executive Sales View
--This is your "One-Stop-Shop." Instead of Power BI having to look at three different tables, it looks at this one view.

CREATE VIEW Gold.v_ExecutiveSalesOverview AS
SELECT 
    s.OrderNumber,
    s.OrderDate,
    p.ProductName,
    p.Category,
    p.Brand,
    c.Name AS CustomerName,
    c.Country AS CustomerCountry,
    s.Quantity,
    s.DeliveryDays,
    
    /* WHY: We calculate Revenue, Cost, and Profit in SQL to ensure 'One Version of the Truth.'
       If we do this in Power BI, different users might use different formulas. 
       Doing it here ensures everyone sees the same numbers.
    */
    (s.Quantity * p.UnitPriceUSD) AS GrossRevenue,
    (s.Quantity * p.UnitCostUSD) AS TotalCost,
    ((s.Quantity * p.UnitPriceUSD) - (s.Quantity * p.UnitCostUSD)) AS NetProfit,
    
    /* WHY: Feature Engineering. We are creating a new 'Business Metric' (Shipping Status).
       This allows us to create a simple 'Red/Green' visual in Power BI 
       to show if the logistics team is meeting their 5-day goal.
    */
    CASE 
        WHEN s.DeliveryDays > 5 THEN 'Late' 
        WHEN s.DeliveryDays IS NULL THEN 'Pending'
        ELSE 'On-Time' 
    END AS ShippingStatus

FROM Silver.Sales s
JOIN Silver.Products p ON s.ProductKey = p.ProductKey
JOIN Silver.Customers c ON s.CustomerKey = c.CustomerKey;
GO


--2.The Store Performance View
--This view aggregates data specifically for a geographic "Deep Dive."

CREATE VIEW Gold.v_StorePerformance AS
SELECT 
    st.StoreKey,
    st.Country,
    st.State,
    st.StoreAgeYears,
    
    /* WHY: Pre-aggregating these counts makes Power BI visuals load much faster.
       Instead of Power BI scanning millions of rows in the Sales table, 
       it just reads these summarized numbers.
    */
    COUNT(s.OrderNumber) AS TotalOrders,
    SUM(s.Quantity) AS TotalItemsSold,
    ROUND(AVG(CAST(s.DeliveryDays AS FLOAT)), 2) AS AvgStoreDeliveryTime

FROM Silver.Stores st
/* WHY: We use LEFT JOIN so we still see stores that have ZERO sales. 
   If we used a regular JOIN, we would 'hide' failing stores, 
   which would be a major mistake for a Data Analyst.
*/
LEFT JOIN Silver.Sales s ON st.StoreKey = s.StoreKey
GROUP BY st.StoreKey, st.Country, st.State, st.StoreAgeYears;
GO

/*
I used the Gold Layer to perform Data Virtualization. By creating views that handle the complex 
JOINs and financial logic at the database level, I reduced the complexity of my
Power BI DAX and improved the overall report performance." 
*/

