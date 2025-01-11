-- Create Tables
CREATE TABLE Customers (
   Customer_ID INTEGER PRIMARY KEY,
   Age_Group VARCHAR(255),
   Income_Level VARCHAR(255),
   Credit_Rating INTEGER
);

CREATE TABLE Credits (
   Credit_ID INTEGER PRIMARY KEY AUTOINCREMENT,
   Customer_ID INTEGER,
   Credit_Amount INTEGER,
   Interest_Rate DECIMAL(3,1),
   Payment_Delays INTEGER,
   FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE Credit_Risks (
   Risk_ID INTEGER PRIMARY KEY AUTOINCREMENT,
   Credit_Rating INTEGER,
   Risk_Level VARCHAR(255)
);

-- Insert into Customers
INSERT INTO Customers (Customer_ID, Age_Group, Income_Level, Credit_Rating) VALUES
(1, 'AgeGroup1', 'IncomeLevel1', 600),
(2,'AgeGroup1', 'IncomeLevel1', 600),
(3,'AgeGroup1', 'IncomeLevel1', 600),
(4,'AgeGroup1', 'IncomeLevel1', 600),
(5,'AgeGroup2', 'IncomeLevel2', 601),
(6,'AgeGroup3', 'IncomeLevel3', 602),
(7, '*****', 'IncomeLevel4', 603),
(8, 'AgeGroup2', 'IncomeLevel1', 604),
(9, 'AgeGroup3', 'IncomeLevel2', 605),
(10, 'AgeGroup1', 'IncomeLevel3', 606),
(11, 'AgeGroup2', 'IncomeLevel4', 607),
(12,'AgeGroup3', 'IncomeLevel1', 608),
(13,'AgeGroup1', 'IncomeLevel2', -100),
(14, 'AgeGroup2', 'IncomeLevel3', 610),
(15, '***', 'IncomeLevel4', 611),
(16, 'AgeGroup1', 'IncomeLevel1', 612),
(17, 'AgeGroup2', 'IncomeLevel2', 613),
(18, 'AgeGroup3', 'IncomeLevel3', 614),
(19, 'AgeGroup1', 'IncomeLevel4', -200),
(20, 'AgeGroup2', 'IncomeLevel1', 616),
(21, 'QQQQ', 'IncomeLevel2', 617),
(22, 'AgeGroup1', 'IncomeLevel3', 618),
(23, 'AgeGroup2', 'IncomeLevel4', 619),
(24, 'AgeGroup3', 'IncomeLevel1', 620),
(25, 'AgeGroup1', 'IncomeLevel2', 621),
(26, 'AgeGroup2', 'IncomeLevel3', 622),
(27, 'AgeGroup3', 'IncomeLevel4', 623),
(28, 'AgeGroup1', 'IncomeLevel1', 624),
(29, 'AgeGroup1', 'IncomeLevel1', 624),
(30, 'AgeGroup1', 'IncomeLevel1', 624),
(31, 'AgeGroup1', 'IncomeLevel1', 624),
(32, 'AgeGroup2', 'IncomeLevel2', 625),
(33, 'AgeGroup3', 'IncomeLevel3', 626),
(34, 'AgeGroup1', 'IncomeLevel4', 627),
(35, 'AgeGroup2', 'IncomeLevel1', 628),
(36, 'Wwwwww', 'IncomeLevel2', 629),
(37, 'AgeGroup1', 'IncomeLevel3', 630),
(38, 'AgeGroup2', 'IncomeLevel4', 631),
(39, 'AgeGroup3', 'IncomeLevel1', 632),
(40, 'AgeGroup1', 'IncomeLevel2', 633),
(41, 'AgeGroup2', 'IncomeLevel3', 634),
(42, '@@@@@', 'IncomeLevel4', 635),
(43, 'AgeGroup1', 'IncomeLevel1', 636),
(44, 'AgeGroup2', 'IncomeLevel2', 637),
(45, 'AgeGroup3', 'IncomeLevel3', 638),
(46, 'AgeGroup1', 'IncomeLevel4', 639),
(47, 'AgeGroup2', 'IncomeLevel1', 640),
(48, 'AgeGroup3', 'IncomeLevel2', 641),
(49, 'AgeGroup1', 'IncomeLevel3', 642),
(50, 'AgeGroup2', 'IncomeLevel4', 643),
(51, 'AgeGroup3', 'IncomeLevel1', 644);

-- Insert into Credits
INSERT INTO Credits (Customer_ID, Credit_Amount, Interest_Rate, Payment_Delays) VALUES
(1, 1000, 3.5, 0),
(2, 2000, 4.5, -1),
(3, 3000, 5.5, 2),
(4, 4000, 6.5, 3),
(5, 5000, 7.5, 0),
(6, 6000, 3.5, 1),
(7, 7000, 4.5, 2),
(8, 8000, 5.5, 3),
(9, 9000, 6.5, 0),
(10, 10000, 7.5, 1),
(11, 11000, 3.5, 2),
(12, 12000, 4.5, -3),
(13, 13000, 5.5, 0),
(14, 14000, 6.5, 1),
(15, 15000, 7.5, 2),
(16, 16000, 3.5, 3),
(17, 17000, 4.5, 0),
(18, 18000, 5.5, -1),
(19, 19000, 6.5, 2),
(20, 20000, 7.5, 3),
(21, 21000, 3.5, 0),
(22, 22000, 4.5, 1),
(23, 23000, 5.5, 2),
(24, 24000, 6.5, -3),
(25, 25000, 7.5, 0),
(26, 26000, 3.5, 1),
(27, 27000, 4.5, 2),
(28, 28000, 5.5, 3),
(29, 29000, 6.5, 0),
(30, 30000, 7.5, 1),
(31, 31000, 3.5, 2),
(32, 32000, 4.5, 3),
(33, 33000, 5.5, 0),
(34, 34000, 6.5, 1),
(35, 35000, 7.5, 2),
(36, 36000, 3.5, -3),
(37, 37000, 4.5, 0),
(38, 38000, 5.5, 1),
(39, 39000, 6.5, 2),
(40, 40000, 7.5, 3),
(41, 41000, 3.5, 0),
(42, 42000, 4.5, 1),
(43, 43000, 5.5, 2),
(44, 44000, 6.5, 3),
(45, 45000, 7.5, 0),
(46, 46000, 3.5, 1),
(47, 47000, 4.5, 2),
(48, 48000, 5.5, 3),
(49, 49000, 6.5, 0),
(50, 50000, 7.5, 1);

-- Insert into Credit_Risks
INSERT INTO Credit_Risks (Credit_Rating, Risk_Level) VALUES
(600, 'Medium'),
(601, 'Low'),
(602, 'Low'),
(603, 'Low'),
(604, 'High'),
(605, 'Low'),
(606, 'Low'),
(607, 'med'),
(608, 'Low'),
(609, 'mediUm'),
(610, 'Medium'),
(611, 'Medium'),
(612, 'lower'),
(613, 'High'),
(614, 'Low'),
(615, 'Low'),
(616, 'High'),
(617, 'Low'),
(618, 'High'),
(619, 'High'),
(620, 'High'),
(621, 'Low'),
(622, 'Medium'),
(623, 'High'),
(624, 'Medium'),
(625, 'Medium'),
(626, 'Low'),
(627, 'High'),
(628, 'Low'),
(629, 'Medium'),
(630, 'Low'),
(631, 'Low'),
(632, 'High'),
(633, 'medium'),
(634, 'Low'),
(635, 'Low'),
(636, 'High'),
(637, 'High'),
(638, 'high'),
(639, 'Medium'),
(640, 'High'),
(641, 'Low'),
(642, 'High'),
(643, 'High'),
(644, 'Medium'),
(645, 'Medium'),
(646, 'High'),
(647, 'hIGH'),
(648, 'High'),
(649, 'High');


-- First, let's segment customers by risk level, interest rate and delays:

WITH CustomerSegments AS (
  SELECT 
    c.Customer_ID,
    cr.Risk_Level,
    CASE 
      WHEN cr2.Interest_Rate < 5.0 THEN 'Low Interest'
      WHEN cr2.Interest_Rate < 6.5 THEN 'Medium Interest'
      ELSE 'High Interest'
    END AS Interest_Category,
    CASE 
      WHEN cr2.Payment_Delays <= 0 THEN 'No Delays'
      WHEN cr2.Payment_Delays <= 2 THEN 'Minor Delays'
      ELSE 'Major Delays'
    END AS Delay_Category
  FROM Customers c
  JOIN Credit_Risks cr ON c.Credit_Rating = cr.Credit_Rating
  JOIN Credits cr2 ON c.Customer_ID = cr2.Customer_ID
)
SELECT 
  Risk_Level,
  Interest_Category,
  Delay_Category,
  COUNT(*) as Customer_Count
FROM CustomerSegments
GROUP BY Risk_Level, Interest_Category, Delay_Category
ORDER BY Risk_Level, Interest_Category, Delay_Category;



-- These queries will provide comprehensive insights into:

-- Customer segmentation based on multiple factors
-- Risk distribution across different customer groups
-- Payment behavior patterns
-- Credit amount and interest rate relationships
-- Statistical analysis of risk and payment delays
-- Age group risk distribution


-- Let's analyze risk assessment by age group and income level:

SELECT 
  c.Age_Group,
  c.Income_Level,
  cr.Risk_Level,
  COUNT(*) as Customer_Count,
  AVG(cr2.Credit_Amount) as Avg_Credit_Amount,
  AVG(cr2.Payment_Delays) as Avg_Delays
FROM Customers c
JOIN Credit_Risks cr ON c.Credit_Rating = cr.Credit_Rating
JOIN Credits cr2 ON c.Customer_ID = cr2.Customer_ID
WHERE c.Age_Group NOT IN ('*****', '***', 'QQQQ', 'Wwwwww', '@@@@@')
GROUP BY c.Age_Group, c.Income_Level, cr.Risk_Level
ORDER BY c.Age_Group, c.Income_Level, cr.Risk_Level;


-- Payment delays analysis:

SELECT 
  cr.Risk_Level,
  COUNT(*) as Total_Customers,
  COUNT(CASE WHEN c.Payment_Delays > 0 THEN 1 END) as Customers_With_Delays,
  AVG(c.Payment_Delays) as Avg_Delay_Days,
  MAX(c.Payment_Delays) as Max_Delay_Days,
  MIN(c.Payment_Delays) as Min_Delay_Days
FROM Customers cust
JOIN Credit_Risks cr ON cust.Credit_Rating = cr.Credit_Rating
JOIN Credits c ON cust.Customer_ID = c.Customer_ID
GROUP BY cr.Risk_Level
ORDER BY AVG(c.Payment_Delays) DESC;


-- Credit amount and interest rate relationship analysis:

SELECT 
  cr.Risk_Level,
  ROUND(AVG(c.Credit_Amount), 2) as Avg_Credit_Amount,
  ROUND(AVG(c.Interest_Rate), 2) as Avg_Interest_Rate,
  COUNT(*) as Number_of_Customers,
  MIN(c.Credit_Amount) as Min_Credit,
  MAX(c.Credit_Amount) as Max_Credit
FROM Customers cust
JOIN Credit_Risks cr ON cust.Credit_Rating = cr.Credit_Rating
JOIN Credits c ON cust.Customer_ID = c.Customer_ID
GROUP BY cr.Risk_Level
ORDER BY Avg_Credit_Amount DESC;


-- Statistical analysis of risk levels and delays:

WITH RiskStats AS (
  SELECT 
    cr.Risk_Level,
    COUNT(*) as Sample_Size,
    AVG(c.Payment_Delays) as Mean_Delays,
    SQRT(AVG(c.Payment_Delays * c.Payment_Delays) - AVG(c.Payment_Delays) * AVG(c.Payment_Delays)) as StdDev_Delays,
    MIN(c.Payment_Delays) as Min_Delays,
    MAX(c.Payment_Delays) as Max_Delays
  FROM Customers cust
  JOIN Credit_Risks cr ON cust.Credit_Rating = cr.Credit_Rating
  JOIN Credits c ON cust.Customer_ID = c.Customer_ID
  GROUP BY cr.Risk_Level
)
SELECT 
  Risk_Level,
  Sample_Size,
  ROUND(Mean_Delays, 2) as Mean_Delays,
  ROUND(StdDev_Delays, 2) as StdDev_Delays,
  Min_Delays,
  Max_Delays
FROM RiskStats
ORDER BY Mean_Delays DESC;


-- Age group and risk level distribution:

SELECT 
  c.Age_Group,
  cr.Risk_Level,
  COUNT(*) as Customer_Count,
  ROUND(AVG(c2.Credit_Amount), 2) as Avg_Credit_Amount,
  ROUND(AVG(c2.Payment_Delays), 2) as Avg_Delays
FROM Customers c
JOIN Credit_Risks cr ON c.Credit_Rating = cr.Credit_Rating
JOIN Credits c2 ON c.Customer_ID = c2.Customer_ID
WHERE c.Age_Group NOT IN ('*****', '***', 'QQQQ', 'Wwwwww', '@@@@@')
GROUP BY c.Age_Group, cr.Risk_Level
ORDER BY c.Age_Group, cr.Risk_Level;

