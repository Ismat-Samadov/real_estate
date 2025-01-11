-- Create Customers Table
CREATE TABLE Customers (
    Customer_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Age_Group VARCHAR(255),
    Gender VARCHAR(10),
    Location VARCHAR(255),
    Total_Spend INTEGER
);

-- Create Purchases Table
CREATE TABLE Purchases (
    Purchase_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Customer_ID INTEGER,
    Product_ID INTEGER,
    Purchase_Amount INTEGER,
    Purchase_Date DATE,
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

-- Insert into Customers Table
INSERT INTO Customers (Age_Group, Gender, Location, Total_Spend) VALUES
('AgeGroup1', 'Male', 'Location1', 3091),
('AgeGroup1', 'Male', 'Location1', 3091),
('AgeGroup1', 'Male', 'Location1', 3091),
('AgeGroup1', 'Male', 'Location1', 3091),
('AgeGroup1', 'Male', 'Location1', 3091),
('AgeGroup2', 'M', 'Location2', 3299),
('AgeGroup3', 'Female', 'Location3', 4964),
('AgeGroup1', 'F', 'Location4', 3125),
('AgeGroup2', 'M', 'Location5', 2978),
('AgeGroup3', 'female', 'Location6', 3522),
('AgeGroup1', 'F', 'Location7', 3207),
('*****', 'M', 'Location8', 1626),
('AgeGroup3', 'F', 'Location9', 2096),
('AgeGroup1', 'male', 'Location10', 2887),
('AgeGroup2', 'F', 'Location11', 1710),
('AgeGroup3', 'F', 'Location12', 1168),
('AgeGroup1', 'F', 'Location13', 2297),
('*****', 'M', 'Location14', 4736),
('AgeGroup3', 'F', 'Location15', 1567),
('AgeGroup1', 'F', 'Location16', 3878),
('AgeGroup2', 'male', 'Location17', 2418),
('AgeGroup3', 'M', 'Location18', 1411),
('AgeGroup1', 'F', 'Location19', 1122),
('AgeGroup2', 'female', 'Location20', 1203),
('AgeGroup3', 'F', 'Location21', 3671),
('AgeGroup1', 'F', 'Location22', 4368),
('AgeGroup2', 'F', 'Location23', 2168),
('AgeGroup3', 'F', 'Location24', 2900),
('AgeGroup1', 'M', 'Location25', 4820),
('AgeGroup2', 'M', 'Location26', 4367),
('AgeGroup3', 'M', 'Location27', 3987),
('AgeGroup1', 'male', 'Location28', 2309),
('AgeGroup2', 'F', 'Location29', 2025),
('AgeGroup3', 'F', 'Location30', 3701),
('AgeGroup1', 'F', 'Location31', 2947),
('*****', 'M', 'Location32', 3688),
('AgeGroup3', 'F', 'Location33', 2617),
('AgeGroup1', 'M', 'Location34', 4895),
('AgeGroup2', 'female', 'Location35', 2322),
('AgeGroup3', 'F', 'Location36', 2847),
('AgeGroup1', 'F', 'Location37', 4535),
('AgeGroup2', 'F', 'Location38', 4059),
(NULL, 'F', 'Location39', 4342),
('AgeGroup1', 'F', 'Location40', 2398),
('AgeGroup2', 'FEMALE', 'Location41', 1859),
('AgeGroup3', 'F', 'Location42', 1122),
('AgeGroup1', 'F', 'Location43', 3273),
('AgeGroup2', 'F', 'Location44', 2294),
(NULL, 'M', 'Location45', 2820),
('AgeGroup1', 'M', 'Location46', 1448),
('AgeGroup2', 'MALE', 'Location47', 4702),
('AgeGroup3', 'F', 'Location48', 2832),
('AgeGroup1', 'M', 'Location49', 2033),
('AgeGroup2', 'F', 'Location50', 1729);

-- Insert into Purchases Table
INSERT INTO Purchases (Customer_ID, Product_ID, Purchase_Amount, Purchase_Date) VALUES
(1, 1, 100, '2099-10-1'),
(2, 2, 120, '2024-10-2'),
(3, 3, 140, '2024-10-3'),
(4, 4, 160, '2024-10-4'),
(5, 5, 180, '2024-10-5'),
(6, 6, 200, '2024-10-6'),
(6, 6, 200, '2024-10-6'),
(6, 6, 200, '2024-10-6'),
(6, 6, 200, '2024-10-6'),
(7, 7, 220, '2024-10-7'),
(8, 8, 240, '2024-10-8'),
(9, 9, 260, '2024-10-9'),
(10, 10, 280, '299-10-10'),
(11, 11, 300, '2024-10-11'),
(12, 12, 320, '2024-10-12'),
(13, 13, 340, '2024-10-13'),
(14, 14, 360, '2024-10-14'),
(15, 15, 380, '2024-10-15'),
(16, 16, 400, '2024-10-16'),
(17, 17, 420, '2024-10-17'),
(18, 18, 440, '1900-10-18'),
(19, 19, 460, '2024-10-19'),
(20, 20, 480, '2024-10-20'),
(21, 21, 500, '2024-10-21'),
(22, 22, 520, '1900-10-22'),
(23, 23, 540, '2024-10-23'),
(24, 24, 560, '2024-10-24'),
(25, 25, 580, '2024-10-25'),
(26, 26, 600, '2024-10-26'),
(27, 27, 620, '2024-10-27'),
(28, 28, 640, '2024-10-28'),
(29, 29, 660, '2024-10-29'),
(30, 30, 680, '2024-10-30'),
(31, 31, 700, '2024-10-1'),
(32, 32, 720, '2024-10-2'),
(33, 33, 740, '2024-10-3'),
(34, 34, 760, '2024-10-4'),
(35, 35, 780, '2024-10-5'),
(36, 36, 800, '2024-10-6'),
(37, 37, 820, '2024-10-7'),
(38, 38, 840, '2024-10-8'),
(39, 39, 860, '2024-10-9'),
(40, 40, 880, '2024-10-10'),
(41, 41, 900, '2024-10-11'),
(41, 41, 900, '2024-10-11'),
(41, 41, 900, '2024-10-11'),
(41, 41, 900, '2024-10-11'),
(41, 41, 900, '2024-10-11'),
(41, 41, 900, '2024-10-11'),
(41, 41, 900, '2024-10-11'),
(42, 42, 920, '2024-10-12'),
(43, 43, 940, '2024-10-13'),
(44, 44, 960, '2024-10-14'),
(45, 45, 980, '2024-10-15'),
(46, 46, 1000, '1800-10-16'),
(47, 47, 1020, '2024-10-17'),
(48, 48, 1040, '2024-10-18'),
(49, 49, 1060, '2024-10-19'),
(50, 50, 1080, '2024-10-20');


-- These queries provide comprehensive analysis of:

-- Customer segmentation
-- Customer lifecycle
-- Product preferences
-- Customer lifetime value
-- Regional analysis
-- Customer loyalty
-- Purchase patterns


-- First, let's clean the data:

-- Standardize Gender values
UPDATE Customers
SET Gender = CASE 
    WHEN UPPER(Gender) IN ('M', 'MALE', 'MALE') THEN 'M'
    WHEN UPPER(Gender) IN ('F', 'FEMALE', 'FEMALE') THEN 'F'
    ELSE Gender
END;

-- Handle invalid Age_Group values
UPDATE Customers
SET Age_Group = 'Unknown'
WHERE Age_Group IS NULL OR Age_Group = '*****';

-- Fix invalid dates
UPDATE Purchases
SET Purchase_Date = '2024-10-01'
WHERE Purchase_Date < '2024-01-01' OR Purchase_Date > '2024-12-31';


-- Customer Segmentation by Purchase Amount, Age Group, and Gender:

WITH CustomerSegments AS (
    SELECT 
        c.Customer_ID,
        c.Age_Group,
        c.Gender,
        COUNT(p.Purchase_ID) as Purchase_Count,
        SUM(p.Purchase_Amount) as Total_Purchase_Amount,
        AVG(p.Purchase_Amount) as Avg_Purchase_Amount
    FROM Customers c
    LEFT JOIN Purchases p ON c.Customer_ID = p.Customer_ID
    WHERE c.Age_Group != 'Unknown'
    GROUP BY c.Customer_ID, c.Age_Group, c.Gender
)
SELECT 
    Age_Group,
    Gender,
    COUNT(*) as Customer_Count,
    ROUND(AVG(Total_Purchase_Amount), 2) as Avg_Total_Spend,
    ROUND(AVG(Purchase_Count), 2) as Avg_Purchase_Frequency,
    ROUND(AVG(Avg_Purchase_Amount), 2) as Avg_Transaction_Value
FROM CustomerSegments
GROUP BY Age_Group, Gender
ORDER BY Age_Group, Gender;

-- Customer Lifecycle Analysis:

SELECT 
    c.Customer_ID,
    MIN(p.Purchase_Date) as First_Purchase,
    MAX(p.Purchase_Date) as Last_Purchase,
    JULIANDAY(MAX(p.Purchase_Date)) - JULIANDAY(MIN(p.Purchase_Date)) as Days_as_Customer,
    COUNT(DISTINCT p.Purchase_ID) as Total_Purchases,
    SUM(p.Purchase_Amount) as Total_Spent,
    ROUND(SUM(p.Purchase_Amount) * 1.0 / COUNT(DISTINCT p.Purchase_ID), 2) as Avg_Purchase_Value
FROM Customers c
JOIN Purchases p ON c.Customer_ID = p.Customer_ID
GROUP BY c.Customer_ID
ORDER BY Total_Spent DESC;

-- Product Trends Analysis:

SELECT 
    p.Product_ID,
    COUNT(*) as Purchase_Count,
    SUM(p.Purchase_Amount) as Total_Revenue,
    AVG(p.Purchase_Amount) as Avg_Purchase_Amount,
    COUNT(DISTINCT c.Customer_ID) as Unique_Customers
FROM Purchases p
JOIN Customers c ON p.Customer_ID = c.Customer_ID
GROUP BY p.Product_ID
ORDER BY Total_Revenue DESC
LIMIT 10;


-- Customer Lifetime Value Analysis:

WITH CustomerValue AS (
    SELECT 
        c.Customer_ID,
        c.Age_Group,
        c.Gender,
        c.Location,
        COUNT(DISTINCT p.Purchase_ID) as Purchase_Frequency,
        SUM(p.Purchase_Amount) as Total_Revenue,
        AVG(p.Purchase_Amount) as Avg_Purchase_Value,
        MAX(p.Purchase_Date) as Last_Purchase_Date
    FROM Customers c
    JOIN Purchases p ON c.Customer_ID = p.Customer_ID
    GROUP BY c.Customer_ID, c.Age_Group, c.Gender, c.Location
)
SELECT 
    Customer_ID,
    Age_Group,
    Gender,
    Location,
    Purchase_Frequency,
    Total_Revenue,
    Avg_Purchase_Value,
    ROUND(Total_Revenue * Purchase_Frequency, 2) as Customer_Value
FROM CustomerValue
ORDER BY Customer_Value DESC;


-- Regional Purchase Behavior Analysis:

SELECT 
    c.Location,
    COUNT(DISTINCT c.Customer_ID) as Total_Customers,
    COUNT(p.Purchase_ID) as Total_Purchases,
    SUM(p.Purchase_Amount) as Total_Revenue,
    ROUND(AVG(p.Purchase_Amount), 2) as Avg_Purchase_Amount,
    ROUND(SUM(p.Purchase_Amount) * 1.0 / COUNT(DISTINCT c.Customer_ID), 2) as Revenue_Per_Customer
FROM Customers c
JOIN Purchases p ON c.Customer_ID = p.Customer_ID
GROUP BY c.Location
ORDER BY Total_Revenue DESC;


-- Customer Loyalty Analysis:

WITH CustomerLoyalty AS (
    SELECT 
        c.Customer_ID,
        COUNT(DISTINCT p.Purchase_ID) as Purchase_Count,
        SUM(p.Purchase_Amount) as Total_Spent,
        ROUND(AVG(p.Purchase_Amount), 2) as Avg_Purchase_Value,
        COUNT(DISTINCT strftime('%Y-%m', p.Purchase_Date)) as Active_Months
    FROM Customers c 
    JOIN Purchases p ON c.Customer_ID = p.Customer_ID
    GROUP BY c.Customer_ID
)
SELECT 
    CASE 
        WHEN Purchase_Count >= 10 THEN 'High Loyalty'
        WHEN Purchase_Count >= 5 THEN 'Medium Loyalty'
        ELSE 'Low Loyalty'
    END as Loyalty_Segment,
    COUNT(*) as Customer_Count,
    ROUND(AVG(Total_Spent), 2) as Avg_Customer_Spend,
    ROUND(AVG(Purchase_Count), 2) as Avg_Purchase_Frequency,
    ROUND(AVG(Active_Months), 2) as Avg_Active_Months
FROM CustomerLoyalty
GROUP BY Loyalty_Segment
ORDER BY Avg_Customer_Spend DESC;