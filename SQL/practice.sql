# Question : find the largest order amount for each salesperson and the associated order number, along with the customer to whom that order belongs to


my answer : 
select * from Customer right join(
select * from Salesperson right join (
select salesperson_id,MAX(Amount) as max,CUST_ID  from Orders group by salesperson_id) as neworders 
on Salesperson.ID =neworders.salesperson_id) as newO 
on newO.CUST_ID=Customer.ID;

Solution :

To extract the sales person id :Step1  : 
select salesperson_id, Number as OrderNum, Amount from Orders 
JOIN (  -- this is our subquery from above:
SELECT salesperson_id, MAX(Amount) AS MaxOrder
FROM Orders
GROUP BY salesperson_id
) as TopOrderAmountsPerSalesperson
USING (salesperson_id)
 where Amount = MaxOrder

Step 2 : Extrcat the Name from second column 
SELECT salesperson_id, Name, 
Orders.Number AS OrderNumber, Orders.Amount
FROM Orders
JOIN Salesperson 
ON Salesperson.ID = Orders.salesperson_id
JOIN (
SELECT salesperson_id, MAX( Amount ) AS MaxOrder
FROM Orders
GROUP BY salesperson_id
) AS TopOrderAmountsPerSalesperson
USING ( salesperson_id ) 
WHERE Amount = MaxOrder


=================
Create Definition, first to practice questions 
=================
create database Trial;
use Trial;
CREATE TABLE Salesperson (
	ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	NAME CHAR(25),
	AGE INT,
	SALARY INT
);

CREATE TABLE Customer (
	ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	Name	varchar(15),
	CITY VARCHAR(15),
	INDUSTRYTYPE VARCHAR(2)
     
);

CREATE TABLE Orders (
	NUMBER INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	ORDER_DATE varchar(30),
	CUST_ID INT,
    salesperson_id INT,
    AMOUNT INT,
	FOREIGN KEY (CUST_ID) REFERENCES Customer(ID) ON DELETE CASCADE,
    FOREIGN KEY (salesperson_id) REFERENCES Salesperson(ID)	ON DELETE CASCADE
);

alter table Orders  ALTER COLUMN ORDER_DATE new_data_type(size);
ALTER TABLE users AUTO_INCREMENT=10;

INSERT INTO Salesperson 
 (NAME, AGE,SALARY) VALUES
 ('Abe', 61,140000),
 ('Bob', 34,44000),
 ( 'Chris',34,40000 ),
 ( 'Dan', 41,52000),
 ('Ken', 57,115000),
 ('Joe',38,38000 )
 ;
 
 INSERT INTO Customer 
 (NAME,CITY,INDUSTRYTYPE) VALUES
 ('Samsonic', 'pleasant','J'),
 ('Panasung', 'oaktown','J'),
 ( 'Samony','jackson','B' ),
 ( 'Orange', 'Jackson','B') ;
 
 INSERT INTO Orders 
(order_date, cust_id,salesperson_id,Amount) VALUES
('8-2-96 2015-11-05 14:29:36 ',4,2,2400),
('1-30-99',	4,	8	,1800),
('7-14-95',	9	,1,	460),
('1-29-98',	7,	2	,540),
('2-3-98',	6,	7	,600),
('3-2-98',	6	,7,	720),
('5-6-98',	9	,7	,150);
 
 INSERT INTO Orders 
(order_date, cust_id,salesperson_id,Amount) VALUES
('8-2-96',4,2,2400);
==================================================
