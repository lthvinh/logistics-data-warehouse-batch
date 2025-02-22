
CREATE TABLE Dim_Users (
    User_Id INT,
    User_Full_Name VARCHAR(255) NOT NULL,
    User_Email VARCHAR(255) UNIQUE NOT NULL,
    User_Password_Hash VARCHAR(255) NOT NULL,
    User_Phone_Number VARCHAR(20) UNIQUE NOT NULL,
    User_Address VARCHAR(255) NOT NULL,
    User_Role ENUM('user', 'driver') NOT NULL,
    User_Created_At DATETIME
)
;

INSERT INTO Dim_Users (
    User_Key
    , User_Id
    , User_Full_Name
    , User_Email
    , User_Password_Hash
    , User_Phone_Number
    , User_Address
    , User_Role
    , User_created_At
) VALUES (
    sha2(concat_ws('|', -1, 'No User', 'no.user@example.com', 'no_hash_password', '0000000000', 'N/A', 'user', CURRENT_TIMESTAMP()), 256)
    , -1
    , 'No User'
    , 'no.user@example.com'
    , 'no_hash_password'
    , '0000000000'
    , 'N/A'
    , 'user'
    , CURRENT_TIMESTAMP()
)
;

CREATE TABLE Dim_Drivers (
    Driver_Id INT,
    Driver_Full_Name VARCHAR(255) NOT NULL,
    Driver_Phone_Number VARCHAR(20) UNIQUE NOT NULL,
    Driver_Email  VARCHAR(255) UNIQUE NOT NULL,
    Driver_Password_Hash VARCHAR(255) NOT NULL,
    Driver_Address VARCHAR(255) NOT NULL,
    Vehicle_License_Plate VARCHAR(20) NOT NULL,
    Vehicle_Type VARCHAR(50),
    Vehicle_Year INT
)
;

CREATE TABLE Dim_Addresses(
    Address_Id CHAR(64) PRIMARY KEY
    , `Address` VARCHAR(255) NOT NULL
)
;

INSERT INTO Dim_Addresses (
    Address_Id
    , Address
) VALUES (
    sha2('No Address Available', 256)
    , 'No Address Available' 
)
;

CREATE VIEW Dim_PickupAddresses AS
SELECT
    Address_Id as Pickup_Address_Id
    , `Address` as Pickup_Address

FROM
    Dim_Addresses
;


CREATE VIEW Dim_DeliveryAddresses AS
SELECT
    Address_Id as Delivery_Id
    , `Address` as Delivery_Address
FROM
    Dim_Addresses
;

CREATE TABLE Dim_Package_Description (
    Package_Description_Id CHAR(64) PRIMARY KEY
    , Package_Description VARCHAR(255)
)
;


CREATE TABLE Dim_Date (
    Date_Key INT PRIMARY KEY
    , `Date` DATE
    , `Year` YEAR
    , Quarter TINYINT
    , Quarter_Name CHAR(2)
    , `Month` TINYINT
    , Month_Name VARCHAR(10)
    , Month_Name_Short CHAR(3)
    , `Day` TINYINT
    , `Weekday` TINYINT
    , Weekday_Name VARCHAR(10)
    , Weekday_Name_Short CHAR(3)
    , Date_Of_Year SMALLINT
    , Week_With_4_Or_More_Days_In_Year TINYINT
    , Week_With_Monday_In_Year TINYINT
    , YYYYMM MEDIUMINT
    , `Year_Month` VARCHAR(10)
    , Is_Weekday VARCHAR(10)
)
;

;
INSERT INTO Dim_Date
WITH RECURSIVE CTE(date) AS (
    SELECT
        CAST('2024-01-01' as DATE)
    UNION ALL
    SELECT
        date + INTERVAL 1 DAY
    FROM
        CTE
    WHERE
        date < '2026-01-01'
)

SELECT
    DATE_FORMAT(`date`, '%Y%m%d') as Date_Key
    , `date`as `Date`
    , YEAR(`date`) as `Year`
    , QUARTER(`date`) as Quarter
    , CONCAT('Q', QUARTER(`date`)) as Quarter_Name
    , MONTH(`date`) as `Month`
    , MONTHNAME(`date`) as Month_Name
    , DATE_FORMAT(`date`, '%b') as Month_Name_Short
    , DAY(`date`) as `Day`
    , WEEKDAY(`date`) + 1 as `Weekday`
    , DATE_FORMAT(`date`, '%W') as Weekday_Name
    , LEFT(DATE_FORMAT(`date`, '%W'), 3) as Weekday_Name_Short
    , DAYOFYEAR(`date`) as Date_Of_Year
    , WEEK(`date`, 3) as Week_With_4_Or_More_Days_In_Year
    , WEEK(`date`, 7) as Week_With_Monday_In_Year
    , DATE_FORMAT(`date`, '%Y%m') as YYYYMM
    , CONCAT(DATE_FORMAT(`date`, '%b'), '_', CAST(MONTH(`date`) AS UNSIGNED)) as `Year_Month`
    , if(WEEKDAY(`date`) < 5, 'Weekday', 'Weekend') as Is_Weekday
FROM
    CTE
;

CREATE VIEW Dim_CreatedDate as
SELECT
    Date_Key AS Created_Date_Key
    , `Date` AS Created_Date
    , `Year` AS Created_Year
    , Quarter AS Created_Quarter
    , Quarter_Name AS Created_Quarter_Name
    , `Month` AS Created_Month
    , Month_Name AS Created_Month_Name
    , Month_Name_Short AS Created_Month_Name_Short
    , `Day` AS Created_Day
    , `Weekday` AS Created_Weekday
    , Weekday_Name AS Created_Weekday_Name
    , Weekday_Name_Short AS Created_Weekday_Name_Short
    , Date_Of_Year AS Created_Day_Of_Year
    , Week_With_4_Or_More_Days_In_Year AS Created_Week_Of_Year
    , Week_With_Monday_In_Year AS Created_Week_With_Monday_In_Year
    , YYYYMM AS Created_YYYYMM
    , `Year_Month` AS Created_Year_Month
    , Is_Weekday AS Created_Is_Weekday
FROM
    Dim_Date
;

CREATE VIEW Dim_AcceptedDate as
SELECT
    Date_Key AS Accepted_Date_Key
    , `Date` AS Accepted_Date
    , `Year` AS Accepted_Year
    , Quarter AS Accepted_Quarter
    , Quarter_Name AS Accepted_Quarter_Name
    , `Month` AS Accepted_Month
    , Month_Name AS Accepted_Month_Name
    , Month_Name_Short AS Accepted_Month_Name_Short
    , `Day` AS Accepted_Day
    , `Weekday` AS Accepted_Weekday
    , Weekday_Name AS Accepted_Weekday_Name
    , Weekday_Name_Short AS Accepted_Weekday_Name_Short
    , Date_Of_Year AS Accepted_Day_Of_Year
    , Week_With_4_Or_More_Days_In_Year AS Accepted_Week_Of_Year
    , Week_With_Monday_In_Year AS Accepted_Week_With_Monday_In_Year
    , YYYYMM AS Accepted_YYYYMM
    , `Year_Month` AS Accepted_Year_Month
    , Is_Weekday AS Accepted_Is_Weekday
FROM
    Dim_Date
;

CREATE VIEW Dim_InTransitDate as
SELECT
    Date_Key AS In_Transit_Date_Key
    , `Date` AS In_Transit_Date
    , `Year` AS In_Transit_Year
    , Quarter AS In_Transit_Quarter
    , Quarter_Name AS In_Transit_Quarter_Name
    , `Month` AS In_Transit_Month
    , Month_Name AS In_Transit_Month_Name
    , Month_Name_Short AS In_Transit_Month_Name_Short
    , `Day` AS In_Transit_Day
    , `Weekday` AS In_Transit_Weekday
    , Weekday_Name AS In_Transit_Weekday_Name
    , Weekday_Name_Short AS In_Transit_Weekday_Name_Short
    , Date_Of_Year AS In_Transit_Day_Of_Year
    , Week_With_4_Or_More_Days_In_Year AS In_Transit_Week_Of_Year
    , Week_With_Monday_In_Year AS In_Transit_Week_With_Monday_In_Year
    , YYYYMM AS In_Transit_YYYYMM
    , `Year_Month` AS In_Transit_Year_Month
    , Is_Weekday AS In_Transit_Is_Weekday
FROM
    Dim_Date
;

CREATE VIEW Dim_DeliveredDate as
SELECT
    Date_Key AS Delivered_Date_Key
    , `Date` AS Delivered_Date
    , `Year` AS Delivered_Year
    , Quarter AS Delivered_Quarter
    , Quarter_Name AS Delivered_Quarter_Name
    , `Month` AS Delivered_Month
    , Month_Name AS Delivered_Month_Name
    , Month_Name_Short AS Delivered_Month_Name_Short
    , `Day` AS Delivered_Day
    , `Weekday` AS Delivered_Weekday
    , Weekday_Name AS Delivered_Weekday_Name
    , Weekday_Name_Short AS Delivered_Weekday_Name_Short
    , Date_Of_Year AS Delivered_Day_Of_Year
    , Week_With_4_Or_More_Days_In_Year AS Delivered_Week_Of_Year
    , Week_With_Monday_In_Year AS Delivered_Week_With_Monday_In_Year
    , YYYYMM AS Delivered_YYYYMM
    , `Year_Month` AS Delivered_Year_Month
    , Is_Weekday AS Delivered_Is_Weekday
FROM
    Dim_Date
;

CREATE VIEW Dim_DeliveryDate as
SELECT
    Date_Key AS Delivery_Date_Key
    , `Date` AS Delivery_Date
    , `Year` AS Delivery_Year
    , Quarter AS Delivery_Quarter
    , Quarter_Name AS Delivery_Quarter_Name
    , `Month` AS Delivery_Month
    , Month_Name AS Delivery_Month_Name
    , Month_Name_Short AS Delivery_Month_Name_Short
    , `Day` AS Delivery_Day
    , `Weekday` AS Delivery_Weekday
    , Weekday_Name AS Delivery_Weekday_Name
    , Weekday_Name_Short AS Delivery_Weekday_Name_Short
    , Date_Of_Year AS Delivery_Day_Of_Year
    , Week_With_4_Or_More_Days_In_Year AS Delivery_Week_Of_Year
    , Week_With_Monday_In_Year AS Delivery_Week_With_Monday_In_Year
    , YYYYMM AS Delivery_YYYYMM
    , `Year_Month` AS Delivery_Year_Month
    , Is_Weekday AS Delivery_Is_Weekday
FROM
    Dim_Date
;

CREATE TABLE Fact_Orders (
    Order_Id INT
    , User_Id INT
    , PickUp_Address_Id CHAR(64)
    , Delivery_Address_Id CHAR(64)
    , Package_Description_Id CHAR(64)
    , Created_Date_Key INT
    , Created_Time TIME
    , Accepted_Date_Key INT
    , Accepted_Time TIME
    , In_Transit_Date_Key INT
    , In_Transit_Time TIME
    , Delivered_Date_Key INT
    , Delivered_Time TIME
    , Delivery_Date_Key INT
    , Delivery_Time TIME
    , Package_Weight FLOAT
    , Created_To_Accepted_Lag TIME
    , Accepted_To_In_Transit_Lag TIME
    , In_Transit_To_Delivered_Lag TIME
    , Delivered_And_Delivery_Difference TIME
    , CONSTRAINT fk__Fact_Orders__User_Key FOREIGN KEY (User_Id) REFERENCES Dim_Users(User_Id)
    , CONSTRAINT fk__Fact_Orders__PickUp_Address_Key FOREIGN KEY (PickUp_Address_Id) REFERENCES Dim_Addresses(Address_Id)
    , CONSTRAINT fk__Fact_Orders__Delivery_Address_Key FOREIGN KEY (Delivery_Address_Id) REFERENCES Dim_Addresses(Address_Id)
    , CONSTRAINT fk__Fact_Orders__Package_Description_Key FOREIGN KEY (Delivery_Address_Id) REFERENCES Dim_PackageDescription(Delivery_Address_Id)
    , CONSTRAINT fk__Fact_Orders__Created_Date_Key FOREIGN KEY (Created_Date_Key) REFERENCES Dim_Date(Date_Key)
    , CONSTRAINT fk__Fact_Orders__Accepted_Date_Key FOREIGN KEY (Accepted_Date_Key) REFERENCES Dim_Date(Date_Key)
    , CONSTRAINT fk__Fact_Orders__In_Transit_Date_Key FOREIGN KEY (In_Transit_Date_Key) REFERENCES Dim_Date(Date_Key)
    , CONSTRAINT fk__Fact_Orders__Delivered_Date_Key FOREIGN KEY (Delivered_Date_Key) REFERENCES Dim_Date(Date_Key)
    , CONSTRAINT fk__Fact_Orders__Delivery_Date_Key FOREIGN KEY (Delivery_Date_Key) REFERENCES Dim_Date(Date_Key)
)
