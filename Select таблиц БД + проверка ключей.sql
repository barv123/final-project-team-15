
--Таблицы нормаизованной базы данных
select * from delivery_dds.drivers;
select * from delivery_dds.items;
select * from delivery_dds.order_items;
select * from delivery_dds.orders;
select * from delivery_dds.stores;
select * from delivery_dds.users;

--Витрины-отчеты:
select * from delivery_dma.orders_report;
select * from delivery_dma.items_report;


------------------------------ Проверки ключей: --------------------------------
--В скриптах ниже подсчитывается кол-во записей с каждым ключом таблицы. 
--Если таблица пустая, то ключ соблюден.  
--Скрипты ниже выдают пустые таблицы, значит дублей в таблицах нет.

--Таблица drivers - Курьеры
--ключ driver_id 
--Дублей нет
select 
	driver_id
from delivery_dds.drivers
group by driver_id
having count(*)  >1;

--Таблица items - Товары
--ключ item_id
--Дублей нет
select
	item_id
from delivery_dds.items
group by item_id
having count(*)  >1;

--Таблица order_items - Связь товаров и заказов.
--Ключ order_id, item_id
--Дублей нет
select 
	order_id
	,item_id
from delivery_dds.order_items
group by order_id, item_id
having count(*)  >1;

-- Таблица orders - Заказы
-- Ключ order_id
-- Дублей нет
select 
	order_id
from delivery_dds.orders
group by order_id
having count(*)  >1;

--Таблица stores - Магазины
--ключ store_id
--Дублей нет
select 
	store_id
from delivery_dds.stores
group by store_id
having count(*)  >1;

--Таблица users - Пользователи
--ключ user_id
--Дублей нет
select 
	user_id
from delivery_dds.users
group by user_id
having count(*)  >1;

--Таблица orders_report - Отчет по заказам на дату
--ключ report_date, store_id
--Дублей нет
select 
	report_date 
	,store_id
from delivery_dma.orders_report
group by report_date, store_id
having count(*)  >1;

--Таблица items_report - Отчет по товарам на дату
--ключ report_date, store_id, item_id
--Дублей нет
select 
	report_date
	,store_id
	,item_id
from delivery_dma.items_report
group by report_date, store_id, item_id
having count(*)  >1;

