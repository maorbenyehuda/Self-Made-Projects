--This Will Be A Query Page With Genral/Interesting Queries To Get An Understanding On This Database
--Here I Will Use SQL Ways To Analize It, With Different Methhods.

--First, Few Tables To See The Data:
--Customer Table
Select *
From Data.Customer

--Orders Table
Select *
From Data.Orders

--Order Rows Table
Select *
From Data.OrderRows

--Date Table
Select *
From Data.Date

--Store Table
Select *
From Data.Store

--Product Table
Select *
From Data.Product


--Now, Let's Go For Few Group By's:
--The Average Product Costs And Prices Among Brands, With Their Profit 
Go
Select Brand,AVG([Unit Cost]) As [Average Cost],AVG([Unit Price]) AS [Average Price],
	AVG([Unit Price])-AVG([Unit Cost]) as Profit
From Data.Product
Group By Brand
Go

--The Amount Of Customers By Country
Go
Select CountryFull,Count(*) as [Amount Of Customers]
From Data.Customer
Group By CountryFull
Order By Count(*) Desc
Go


--Max And Min Of Prices And Costs Among Products Categories
Go
Select Category,Max([Unit Cost]) As [Max Cost],Max([Unit Price]) AS [Max Price]
	,Min([Unit Cost]) As [Min Cost],Min([Unit Price]) AS [Min Price]
From Data.Product
Group By Category
Go


--Amount Of Working Days Among Week Days
Go
Select [Day of Week],Sum(Cast([Working Day] As int)) As [Amount Of Days]
From Data.Date
Group By [Day of Week]
Go


--Products Colors Count
Go
Select p.Color,Count(*) as [Color Count]
From Data.Product p 
Group By p.Color
Order By Count(*) Desc
Go


--Now Let's Add Some Joins
--Amount Of Orders In Each Day
Go
Select [Day of Week],Count(Distinct o.OrderKey) As [Amount Of Days]
From Data.Date d join Data.Orders o on d.Date=o.[Order Date]
Group By [Day of Week]
Order By Count(*) Desc
Go


--Top 10 Products in Orders
Go
Select Top 10 p.[Product Name],Count(*) as [Count Of Orders]
From Data.Product p join Data.OrderRows orr
	on p.ProductKey=orr.ProductKey
Group By p.ProductKey,p.[Product Name]
Order By Count(*) Desc
Go


--Top 10 products amount in orders
Go
Select Top 10 p.[Product Name],Sum(orr.Quantity) as [Count Of Orders]
From Data.Product p join Data.OrderRows orr
	on p.ProductKey=orr.ProductKey
Group By p.ProductKey,p.[Product Name]
Order By Sum(orr.Quantity) Desc
Go


--Count of Orders By Dates
Go
Select Day(o.[Order Date])as [Day],Month(o.[Order Date]) as [Month],Count(*) [Count Of Orders]
From Data.Date d join Data.Orders o on d.Date=o.[Order Date]
Group By Day(o.[Order Date]),Month(o.[Order Date])
Order By Count(*) Desc
Go


--Difference in Amount Of Orders Among Years
Go
Select Year(o.[Order Date]) as [Year],Count(*) as [Count Of Orders]
From Data.Orders as o
Group By Year(o.[Order Date])
Order By Year(o.[Order Date])
Go



--Orders Among Stores
Go
Select s.[Store Code],s.Name,Count(*) as [Count Of Orders]
From Data.Store s join Data.Orders o on s.StoreKey=o.StoreKey
Group By s.[Store Code],s.Name
Order By Count(*) desc
Go


--Difference Between the product price/cost, and the product price/cost in the order
Go
Select p.[Product Name],p.[Unit Cost] as [Product unit cost],p.[Unit Price] as [Product unit price],orr.[Unit Price]as [order unit Price],orr.[Unit Cost] as [order unit cost],orr.Quantity,orr.[Net Price],
	Cast((p.[Unit Cost]-orr.[Unit Cost])/p.[Unit Cost]*100 as Varchar(30))+'%' as [precentage cost diff],
	Cast((p.[Unit Price]-orr.[Unit Price])/p.[Unit Price]*100 as Varchar(30))+'%' as [precentage price diff]
From Data.Product p join Data.OrderRows orr
	on p.ProductKey=orr.ProductKey
Where p.[Unit Cost]!=orr.[Unit Cost] Or p.[Unit Price]!=orr.[Unit Price]
Go



--Now We Will Use Views And Over, Partition By
--The ratio between Customers And Stores Among Countries to the Total Amount Of Customers In That Country
Go
With Customers_Stores_Among_Countries as(
	Select c.CountryFull as [Customer Country],s.Country As [Store Country],Count(Distinct c.CustomerKey) as [Amount of Customers],Count(Distinct s.StoreKey) as [Amount of Stores]
	From Data.Customer c join Data.Orders o on c.CustomerKey=o.CustomerKey
		join Data.Store s on o.StoreKey=s.StoreKey
	Group By s.Country,c.CountryFull
	
)

Select cs.*,c.[Amount of Customers],Cast(cs.[Amount of Customers]*100/c.[Amount of Customers]as varchar(30))+'%' as [Ratio]
From Customers_Stores_Among_Countries cs join (
	Select c.CountryFull as [Customer Country],Count(Distinct c.CustomerKey) as [Amount of Customers] 
	From Data.Customer c 
	Group By c.CountryFull) c on cs.[Customer Country]=c.[Customer Country]
Go


--Orders Among Stores In the Years With Change
Go
with base as(
	Select Year(o.[Order Date]) as [Year],s.[Store Code],s.Name,Count(*) as [Count Of Orders]
	From Data.Store s join Data.Orders o on s.StoreKey=o.StoreKey
	Group By Year(o.[Order Date]),s.[Store Code],s.Name
	
),
new as(
	Select b.[Year],b.[Store Code],b.Name,b.[Count Of Orders],
		Lag(b.[Count Of Orders]) Over (Partition By b.[Store Code] Order By b.[Year]) as PrevCount
	From base b
)

Select n.[Year],n.[Store Code],n.Name,n.[Count Of Orders],n.PrevCount,
	Case 
		When n.PrevCount Is Null or n.PrevCount=0 Then Null
		Else Cast((n.[Count Of Orders]-n.PrevCount)*100/n.PrevCount as Varchar(30))+'%'
	End as [Change Among Years]
From new n
Go


--Customers Among Stores In The Years With Change
GO
with base2 as(
	Select s.[Store Code],s.Name,Year(o.[Order Date]) as [Year],Count(Distinct c.CustomerKey) as [Amount Of Customers]
	From Data.Customer c join Data.Orders o on c.CustomerKey=o.CustomerKey
		join Data.Store s on s.StoreKey=o.StoreKey
	Group By s.[Store Code],s.Name,Year(o.[Order Date])
	
),
new2 as(
	Select b.[Year],b.[Store Code],b.Name,b.[Amount Of Customers],
		Lag(b.[Amount Of Customers]) Over (Partition By b.[Store Code] Order By b.[Year]) as PrevCount
	From base2 b
)

Select n.[Year],n.[Store Code],n.Name,n.[Amount Of Customers],n.PrevCount,
	Case 
		When n.PrevCount Is Null or n.PrevCount=0 Then Null
		Else Cast((n.[Amount Of Customers]-n.PrevCount)*100/n.PrevCount as Varchar(30))+'%'
	End as [Change Among Years]
From new2 n
Order By n.[Store Code],n.[Year]

--
--Customers Among Countries In The Years With Change
GO
with base2 as(
	Select s.[Store Code],s.Name,Year(o.[Order Date]) as [Year],Count(Distinct c.CustomerKey) as [Amount Of Customers]
	From Data.Customer c join Data.Orders o on c.CustomerKey=o.CustomerKey
		join Data.Store s on s.StoreKey=o.StoreKey
	Group By s.[Store Code],s.Name,Year(o.[Order Date])
	
),
new2 as(
	Select b.[Year],b.[Store Code],b.Name,b.[Amount Of Customers],
		Lag(b.[Amount Of Customers]) Over (Partition By b.[Store Code] Order By b.[Year]) as PrevCount
	From base2 b
)

Select n.[Year],n.[Store Code],n.Name,n.[Amount Of Customers],n.PrevCount,
	Case 
		When n.PrevCount Is Null or n.PrevCount=0 Then Null
		Else Cast((n.[Amount Of Customers]-n.PrevCount)*100/n.PrevCount as Varchar(30))+'%'
	End as [Change Among Years]
From new2 n
Order By n.[Store Code],n.[Year]
--

--Functions
-- This Function give all the customers which ordered only from 1 manufacturer
Go
Create Function Customers_Of_Lone_Manufacturer(@Manufacturer Varchar(100))
Returns Table
As
Return(
    SELECT c.CustomerKey
    FROM data.Customer   AS c
    JOIN data.Orders     AS o   ON o.CustomerKey  = c.CustomerKey
    JOIN data.OrderRows  AS orr ON orr.OrderKey   = o.OrderKey
    JOIN data.Product    AS p   ON p.ProductKey   = orr.ProductKey
    GROUP BY c.CustomerKey
    HAVING 
           COUNT(*) > 0
       AND COUNT(*) = COUNT(CASE WHEN p.Manufacturer = @Manufacturer THEN 1 END)
)
Go

Select * From Customers_Of_Lone_Manufacturer('Contoso, Ltd')
Go


--This Function Get All The Orders That Has Only 1 Manufacturer
Go
Create Function Orders_Of_Lone_Manufacturer(@Manufacturer Varchar(100))
Returns Table
As
Return
(
	Select Distinct orr.OrderKey
	From data.OrderRows as orr Join data.Product as p
	on p.ProductKey=orr.ProductKey
	Where p.Manufacturer=@Manufacturer
	And Not Exists (
		Select 1
		From data.OrderRows as orr2 Join data.Product as p2
		on p2.ProductKey=orr2.ProductKey
		Where p2.Manufacturer!=@Manufacturer And orr.OrderKey=orr2.OrderKey
	)
);
Go

Go
Select c.CustomerKey,c.City,c.Continent,c.Gender,c.Name,c.Age
From data.orders o join Customer c on o.CustomerKey=c.CustomerKey
Where o.OrderKey in (Select * From Orders_Of_Lone_Manufacturer('Contoso, Ltd'))
Go



Select *
From Data.CurrencyExchange





























