-- Create Products Table
CREATE TABLE Products (
    Product_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Product_Name TEXT NOT NULL,
    Category TEXT,
    Price REAL
);

-- Create Regions Table
CREATE TABLE Regions (
    Region_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Region_Name TEXT NOT NULL,
    Country TEXT NOT NULL
);

-- Create Sellers Table
CREATE TABLE Sellers (
    Seller_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Seller_Name TEXT NOT NULL,
    Experience_Years INTEGER,
    Sales_Quota REAL
);

-- Create Sales Table
CREATE TABLE Sales (
    Sales_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Product_ID INTEGER,
    Sales_Amount REAL,
    Sales_Date TEXT,
    Region_ID INTEGER,
    Seller_ID INTEGER,
    Cost REAL,
    FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID),
    FOREIGN KEY (Region_ID) REFERENCES Regions(Region_ID),
    FOREIGN KEY (Seller_ID) REFERENCES Sellers(Seller_ID)
);

-- Insert Products Data
INSERT INTO Products (Product_Name, Category, Price) VALUES
('Product1', 'Category1', 100),
('Product2', 'Category2', 110),
('Product3', 'Category3', 120),
('Product4', NULL, 130),
('Product5', 'Category1', 140),
('Product6', 'Category2', 150),
('Product7', 'Category3', 160),
('Product8', 'Category4', 170),
('Product9', 'Category1', 180),
('Product10', 'Category2', 190),
('Product11', NULL, 200),
('Product12', 'Category4', NULL),
('Product13', 'Category1', 220),
('Product14', 'Category2', 230),
('Product15', 'Category3', 240),
('Product16', 'Category4', 250),
('Product17', 'Category1', 260),
('Product18', NULL, 270),
('Product19', 'Category3', 280),
('Product20', 'Category4', 290),
('Product21', 'Category1', 300),
('Product22', 'Category2', 310),
('Product23', 'Category3', 320),
('Product24', 'Category4', 330),
('Product25', 'Category1', NULL),
('Product26', 'Category2', 350),
('Product27', 'Category3', NULL),
('Product28', 'Category4', 370),
('Product29', 'Category1', 380),
('Product30', 'Category2', 390),
('Product31', 'Category3', 400),
('Product32', 'Category4', 410),
('Product33', 'Category1', 420),
('Product34', 'Category2', 430),
('Product35', 'Category3', 440),
('Product36', 'Category4', 450),
('Product37', 'Category1', 460),
('Product38', NULL, 470),
('Product39', 'Category3', 480),
('Product40', 'Category4', NULL),
('Product41', 'Category1', 500),
('Product42', 'Category2', 510),
('Product43', 'Category3', 520),
('Product44', 'Category4', 530),
('Product45', NULL, 540),
('Product46', 'Category2', 550),
('Product47', 'Category3', 560),
('Product48', 'Category4', 570),
('Product49', 'Category1', 580),
('Product50', 'Category2', 590);

-- Insert Regions Data
INSERT INTO Regions (Region_Name, Country) VALUES
('Region1', 'Country1'),
('Region2', 'Country2'),
('Region3', 'Country3'),
('Region4', 'Country4'),
('Region5', 'Country5'),
('Region6', 'Country1'),
('Region7', 'Country2'),
('Region8', 'Country3'),
('Region9', 'Country4'),
('Region10', 'Country5'),
('Region11', 'Country1'),
('Region12', 'Country2'),
('Region13', 'Country3'),
('Region14', 'Country4'),
('Region15', 'Country5'),
('Region16', 'Country1'),
('Region17', 'Country2'),
('Region18', 'Country3'),
('Region19', 'Country4'),
('Region20', 'Country5'),
('Region21', 'Country1'),
('Region22', 'Country2'),
('Region23', 'Country3'),
('Region24', 'Country4'),
('Region25', 'Country5'),
('Region26', 'Country1'),
('Region27', 'Country2'),
('Region28', 'Country3'),
('Region29', 'Country4'),
('Region30', 'Country5'),
('Region31', 'Country1'),
('Region32', 'Country2'),
('Region33', 'Country3'),
('Region34', 'Country4'),
('Region35', 'Country5'),
('Region36', 'Country1'),
('Region37', 'Country2'),
('Region38', 'Country3'),
('Region39', 'Country4'),
('Region40', 'Country5'),
('Region41', 'Country1'),
('Region42', 'Country2'),
('Region43', 'Country3'),
('Region44', 'Country4'),
('Region45', 'Country5'),
('Region46', 'Country1'),
('Region47', 'Country2'),
('Region48', 'Country3'),
('Region49', 'Country4'),
('Region50', 'Country5');

-- Insert Sellers Data
INSERT INTO Sellers (Seller_Name, Experience_Years, Sales_Quota) VALUES
('Seller1', 1, 0),
('Seller2', 2, 500),
('Seller3', 3, 1000),
('Seller4', -999, 1500),
('Seller5', 5, 2000),
('Seller6', 6, 2500),
('Seller7', 7, 3000),
('Seller8', NULL, 3500),
('Seller9', 9, 4000),
('Seller10', 10, 4500),
('Seller11', 1, 5000),
('Seller12', NULL, 5500),
('Seller13', 3, 6000),
('Seller14', 4, 6500),
('Seller15', 5, 7000),
('Seller16', 6, 7500),
('Seller17', NULL, 8000),
('Seller18', 8, 8500),
('Seller19', 9, 9000),
('Seller20', 10, 9500),
('Seller21', -999, 10000),
('Seller22', NULL, 10500),
('Seller23', 3, 11000),
('Seller24', 4, 11500),
('Seller25', 5, 12000),
('Seller26', 6, 12500),
('Seller27', 7, 13000),
('Seller28', 8, 13500),
('Seller29', 9, 14000),
('Seller30', NULL, 14500),
('Seller31', 1, 15000),
('Seller32', 2, 15500),
('Seller33', 3, 16000),
('Seller34', NULL, 16500),
('Seller35', 5, 17000),
('Seller36', 6, 17500),
('Seller37', NULL, 18000),
('Seller38', 8, 18500),
('Seller39', 9, 19000),
('Seller40', 10, 19500),
('Seller41', 1, 20000),
('Seller42', 2, 20500),
('Seller43', 3, 21000),
('Seller44', 4, 21500),
('Seller45', 5, 22000),
('Seller46', 6, 22500),
('Seller47', 7, 23000),
('Seller48', 8, 23500),
('Seller49', 9, 24000),
('Seller50', 10, 24500);

-- Insert Sales Data
INSERT INTO Sales (Product_ID, Sales_Amount, Sales_Date, Region_ID, Seller_ID, Cost) VALUES
(1, 500, '2024-10-01', 1, 1, 100),
(2, 510, '2024-10-02', 2, 2, 120),
(3, 520, '2024-10-03', 3, 3, 140),
(4, 530, '2024-10-04', 4, 4, 160),
(5, 540, '2024-10-05', 1, 5, 180),
(6, 550, '2024-10-06', 2, 1, 200),
(7, 560, '2024-10-07', 3, 2, 220),
(8, 570, '2024-10-08', 4, 3, 240),
(9, 580, '2024-10-09', 1, 4, 260),
(10, 590, '2024-10-10', 2, 5, 280),
(11, 600, '2024-10-11', 3, 1, 300),
(12, 610, '2024-10-12', 4, 2, 320),
(13, 620, '2024-10-13', 1, 3, 340),
(14, 630, '2024-10-14', 2, 4, 360),
(15, 640, '2024-10-15', 3, 5, 380),
(16, 650, '2024-10-16', 4, 1, 400),
(17, 660, '2024-10-17', 1, 2, 420),
(18, 670, '2024-10-18', 2, 3, 440),
(19, 680, '2024-10-19', 3, 4, 460),
(20, 690, '2024-10-20', 4, 5, 480),
(21, 700, '2024-10-21', 1, 1, 500),
(22, 710, '2024-10-22', 2, 2, 520),
(23, 720, '2024-10-23', 3, 3, 540),
(24, 730, '2024-10-24', 4, 4, 560),
(25, 740, '2024-10-25', 1, 5, 580),
(26, 750, '2024-10-26', 2, 1, 600),
(27, 760, '2024-10-27', 3, 2, 620),
(28, 770, '2024-10-28', 4, 3, 640),
(29, 780, '2024-10-29', 1, 4, 660),
(30, 790, '2024-10-30', 2, 5, 680),
(31, 800, '2024-10-01', 3, 1, 700),
(32, 810, '2024-10-02', 4, 2, 720),
(33, 820, '2024-10-03', 1, 3, 740),
(34, 830, '2024-10-04', 2, 4, 760),
(35, 840, '2024-10-05', 3, 5, 780),
(36, 850, '2024-10-06', 4, 1, 800),
(37, 860, '2024-10-07', 1, 2, 820),
(38, 870, '2024-10-08', 2, 3, 840),
(39, 880, '2024-10-09', 3, 4, 860),
(40, 890, '2024-10-10', 4, 5, 880),
(41, 900, '2024-10-11', 1, 1, 900),
(42, 910, '2024-10-12', 2, 2, 920),
(43, 920, '2024-10-13', 3, 3, 940),
(44, 930, '2024-10-14', 4, 4, 960),
(45, 940, '2024-10-15', 1, 5, 980),
(46, 950, '2024-10-16', 2, 1, 1000),
(47, 960, '2024-10-17', 3, 2, 1020),
(48, 970, '2024-10-18', 4, 3, 1040),
(49, 980, '2024-10-19', 1, 4, 1060),
(50, 990, '2024-10-20', 2, 5, 1080);



-- Breakdown of what each section does:

-- Data Quality Checks:

-- Identifies duplicate products
-- Checks for null values in critical fields
-- Identifies invalid seller experience data


-- Advanced Analysis:

-- Top 5 products by sales and revenue
-- Detailed seller performance metrics
-- Regional sales trends over time
-- Product category analysis
-- High variance product identification


-- Advanced Visualizations Support:

-- Regional performance metrics
-- Quota vs achievement analysis
-- Heatmap data generation
-- Monthly trend analysis


-- Data Quality Checks
-- Check for duplicate products
SELECT Product_Name, COUNT(*) as count
FROM Products
GROUP BY Product_Name
HAVING COUNT(*) > 1;

-- Check for null values
SELECT 
    SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) as null_categories,
    SUM(CASE WHEN Price IS NULL THEN 1 ELSE 0 END) as null_prices
FROM Products;

-- Check for invalid experience years
SELECT *
FROM Sellers
WHERE Experience_Years < 0 OR Experience_Years IS NULL;

-- Advanced Data Analysis

-- 1. Top 5 products by sales quantity and revenue
SELECT 
    p.Product_Name,
    p.Category,
    COUNT(*) as sale_count,
    SUM(s.Sales_Amount) as total_revenue,
    AVG(s.Sales_Amount) as avg_revenue
FROM Sales s
JOIN Products p ON s.Product_ID = p.Product_ID
GROUP BY p.Product_ID, p.Product_Name, p.Category
ORDER BY total_revenue DESC
LIMIT 5;

-- 2. Seller Performance Analysis
SELECT 
    s.Seller_Name,
    s.Experience_Years,
    s.Sales_Quota,
    COUNT(*) as total_sales,
    SUM(sa.Sales_Amount) as total_revenue,
    SUM(sa.Sales_Amount - sa.Cost) as total_profit,
    ROUND(SUM(sa.Sales_Amount) / s.Sales_Quota * 100, 2) as quota_achievement_percentage
FROM Sellers s
JOIN Sales sa ON s.Seller_ID = sa.Seller_ID
GROUP BY s.Seller_ID, s.Seller_Name, s.Experience_Years, s.Sales_Quota
ORDER BY total_revenue DESC;

-- 3. Regional Sales Analysis Over Time
SELECT 
    r.Region_Name,
    r.Country,
    strftime('%Y-%m', s.Sales_Date) as month,
    COUNT(*) as sale_count,
    SUM(s.Sales_Amount) as total_revenue
FROM Sales s
JOIN Regions r ON s.Region_ID = r.Region_ID
GROUP BY r.Region_ID, r.Region_Name, r.Country, month
ORDER BY r.Region_Name, month;

-- 4. Product Category Sales Trend
SELECT 
    p.Category,
    strftime('%Y-%m', s.Sales_Date) as month,
    COUNT(*) as sale_count,
    SUM(s.Sales_Amount) as total_revenue,
    AVG(s.Sales_Amount) as avg_revenue
FROM Sales s
JOIN Products p ON s.Product_ID = p.Product_ID
WHERE p.Category IS NOT NULL
GROUP BY p.Category, month
ORDER BY p.Category, month;

-- 5. High Variance Products Analysis
WITH ProductStats AS (
    SELECT 
        p.Product_ID,
        p.Product_Name,
        p.Category,
        AVG(s.Sales_Amount) as avg_sale,
        (SUM(s.Sales_Amount * s.Sales_Amount) / COUNT(*) - (AVG(s.Sales_Amount) * AVG(s.Sales_Amount))) as variance
    FROM Sales s
    JOIN Products p ON s.Product_ID = p.Product_ID
    GROUP BY p.Product_ID, p.Product_Name, p.Category
)
SELECT *
FROM ProductStats
ORDER BY variance DESC
LIMIT 10;

-- Regional Product Category Performance
SELECT 
    r.Region_Name,
    r.Country,
    p.Category,
    COUNT(*) as sale_count,
    SUM(s.Sales_Amount) as total_revenue,
    SUM(s.Sales_Amount - s.Cost) as total_profit,
    AVG(s.Sales_Amount) as avg_sale_amount
FROM Sales s
JOIN Regions r ON s.Region_ID = r.Region_ID
JOIN Products p ON s.Product_ID = p.Product_ID
WHERE p.Category IS NOT NULL
GROUP BY r.Region_ID, r.Region_Name, r.Country, p.Category
ORDER BY total_revenue DESC;

-- Seller Quota vs Achievement Analysis
SELECT 
    s.Seller_Name,
    s.Sales_Quota,
    SUM(sa.Sales_Amount) as total_sales,
    ROUND(SUM(sa.Sales_Amount) / s.Sales_Quota * 100, 2) as achievement_percentage,
    COUNT(*) as number_of_sales,
    AVG(sa.Sales_Amount) as avg_sale_amount
FROM Sellers s
JOIN Sales sa ON s.Seller_ID = sa.Seller_ID
GROUP BY s.Seller_ID, s.Seller_Name, s.Sales_Quota
HAVING s.Sales_Quota > 0
ORDER BY achievement_percentage DESC;

-- Regional Heat Map Data
SELECT 
    r.Region_Name,
    p.Category,
    COUNT(*) as sale_count,
    SUM(s.Sales_Amount) as total_revenue,
    AVG(s.Sales_Amount) as avg_sale_amount,
    MIN(s.Sales_Amount) as min_sale,
    MAX(s.Sales_Amount) as max_sale
FROM Sales s
JOIN Regions r ON s.Region_ID = r.Region_ID
JOIN Products p ON s.Product_ID = p.Product_ID
WHERE p.Category IS NOT NULL
GROUP BY r.Region_ID, r.Region_Name, p.Category
ORDER BY r.Region_Name, total_revenue DESC;

-- Monthly Trend Analysis by Region
SELECT 
    r.Region_Name,
    strftime('%Y-%m', s.Sales_Date) as month,
    COUNT(*) as sale_count,
    SUM(s.Sales_Amount) as total_revenue,
    AVG(s.Sales_Amount) as avg_sale_amount,
    SUM(s.Sales_Amount - s.Cost) as total_profit
FROM Sales s
JOIN Regions r ON s.Region_ID = r.Region_ID
GROUP BY r.Region_ID, r.Region_Name, month
ORDER BY r.Region_Name, month;