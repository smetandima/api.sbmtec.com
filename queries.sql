--create index trans_band_phone_idx on transactions(bandwidth_phone) -- ´´½¨bandwidth phoneµÄ·Ç¾Û¼¯Ë÷Òý£¬Ìá¸ß²éÑ¯ËÙ¶È£¬È±µã»áÔö¼ÓÒ»µãµãµÄÓ²ÅÌÊ¹ÓÃÁ¿(ºöÂÔ²»¼Æ)

/* Top visitors, all customers, customers information */
select
	max(spoonity_id) spoonity_id,
	max(gfo_id) gfo_id,
    max(email) email,
	phone,
	max(birthday) birthday,
	ISNULL(datediff(day, GETDATE(), max(birthday)), -999) days_remaining_until_birthday,
    max(bookkeeping_date) last_visit,
	min(bookkeeping_date) first_visit,
	datediff(day, max(bookkeeping_date), GETDATE()) last_visit_days_ago,
	ISNULL(count(*) * 1.0 / NULLIF(datediff(week, min(bookkeeping_date), max(bookkeeping_date)), 0), count(*)) visit_ratio,
    max(name) name,
    count(*) visit_count
from(
	select
		tsm.clover_id spoonity_id,
		null gfo_id,
		tsm.email email,
		tsm.phone phone,
		tsm.birth_day birthday,
		t.bookkeeping_date,
		concat(tsm.first_name,' ',tsm.last_name) name
	from transactions t
		join trans_spoonity_member tsm on t.id = tsm.transaction_id
		join shops s on s.id = t.shop_id
	where t.delete_timestamp is null
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')

	union all
	select
		clover_id spoonity_id,
		gfo.id gfo_id,
		null email,
		ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) phone,
		null birthday,
		t.bookkeeping_date,
		null name
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join shops s on s.id = t.shop_id
		join(
			select distinct phone,clover_id from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	union all
	select
		clover_id spoonity_id,
		null gfo_id,
		null email,
		ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) phone,
		null birthday,
		t.bookkeeping_date,
		null name
	from transactions t with(index (trans_band_phone_idx))
		join shops s on s.id = t.shop_id and (t.spoonity_member is null or t.spoonity_member='')
		join(
			select distinct phone,first_name,clover_id from trans_spoonity_member
		) p1 on t.bandwidth_phone is not null and t.bandwidth_phone <> '' and ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	-- and  p1.first_name like (select Clover_Card_Hold_Name  from trans_payments where transaction_id = t.id   ) -- Æ¥ÅäÐÕÃû ¸öÈËÈÏÎªÔÝÊ±²»ÒªÆ¥Åä
) T group by T.phone,T.spoonity_id
union all
-- GF ÎÞ·¨¹ØÁªµ½spoonityµÄÊý¾Ý
select
    null spoonity_id,
	max(gfo.id) gfo_id,
    gfo.client_email email,
	ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) phone,
	null birthday,
	null days_remaining_until_birthday,
    max(bookkeeping_date) last_visit,
	min(bookkeeping_date) first_visit,
	datediff(day, max(bookkeeping_date), GETDATE()) last_visit_days_ago,
	ISNULL(count(*) * 1.0 / NULLIF(datediff(week, min(bookkeeping_date), max(bookkeeping_date)), 0), count(*)) visit_ratio,
    concat(max(gfo.client_first_name),' ',max(gfo.client_last_name)) name,
    count(*) visit_count
from transactions t
    join global_food_order gfo on t.global_food_order_id = gfo.id
	join shops s on s.id = t.shop_id
	left join
	(
		select distinct phone from trans_spoonity_member
	) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
where t.delete_timestamp is null
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	and p1.phone is null
group by ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')), gfo.client_email
order by visit_count desc, max(bookkeeping_date) desc
OFFSET 0 rows
FETCH NEXT 500 ROWS ONLY



/* customers counts */
select
	'tsm_customers_count' customer_type,
	count(*) customer_count
from(
	-- ´¿spoonityµÄÊý¾Ý
	select
		1 st
	from transactions t
		join trans_spoonity_member tsm on t.id = tsm.transaction_id
		join shops s on s.id = t.shop_id
	where t.delete_timestamp is null
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	group by tsm.clover_id
)T
union all
-- GF ÎÞ·¨¹ØÁªµ½spoonityµÄÊý¾Ý
select
 'gfo_customers_count' customer_type,
 count(*) customer_count
from
(
	select
	 1 st
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join shops s on s.id = t.shop_id
		left join
		(
			select distinct phone from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		and p1.phone is null
		group by ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1',''))
) t2

/* visits counts */
select 'all_count' as count_type, count(*) total_count
from transactions t
	join shops s on s.id = t.shop_id
where t.delete_timestamp is null
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
union all
select
	'tsm_count' as count_type,
	count(*) as total_count
from
(
	-- ´¿spoonityµÄÊý¾Ý
	select
		1 st
	from transactions t
		join trans_spoonity_member tsm on t.id = tsm.transaction_id
		join shops s on s.id = t.shop_id
	where t.delete_timestamp is null
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	-- global food ÄÜÆ¥Åäµ½spoonityµÄÊý¾Ý£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
	union all
	select
		1 st
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join shops s on s.id = t.shop_id
		join(
			select distinct phone from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	-- clover Í¨¹ýÐÕÃûºÍbandwidthÄÜºÍspoonity Æ¥Åäµ½µÄÊý¾Ý £¬ÐèÒªÅÅ³ýÒÑ¾­ÊÇµÇÂ¼»áÔ±µ«ÓÖÊ¹ÓÃcloverµÄ½»Ò×£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
	union all
	select
		1 st
	from transactions t with(index (trans_band_phone_idx))
		join shops s on s.id = t.shop_id and (t.spoonity_member is null or t.spoonity_member='')
		join(
			select distinct phone,first_name from trans_spoonity_member
		) p1 on t.bandwidth_phone is not null and t.bandwidth_phone <> '' and ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
) t1
union all
select 'gfo_count' as count_type, count(*) total_gfo_count
from
(
	select
	 1 st
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join shops s on s.id = t.shop_id
		left join
		(
			select distinct phone from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		and p1.phone is null
) t2

/* Shop | all_count, tsm_count, gfo_count */
select total.shop_name, total.all_count, tsm.tsm_count, gfo.gfo_count
from (
	select count(*) all_count, max(s.description) shop_name, s.id id
	from transactions t
	join shops s on s.id = t.shop_id
	where t.bookkeeping_date between '2020-12-01' and '2020-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	group by s.id) total
left join (
	-- ´¿spoonityµÄÊý¾Ý
	select count(*) tsm_count,id from
	(
		select   s.id id
		from transactions tt
			join trans_spoonity_member tsm on tt.id = tsm.transaction_id
			join shops s on s.id = tt.shop_id
		where tt.delete_timestamp is null and tt.bookkeeping_date between '2020-12-01' and '2020-12-15'
			and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		-- global food ÄÜÆ¥Åäµ½spoonityµÄÊý¾Ý£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
		union all
		select   s.id id
		from transactions tt
			join global_food_order gfo on tt.global_food_order_id = gfo.id
			join shops s on s.id = tt.shop_id
			join(
				select distinct phone from trans_spoonity_member
			) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
		where tt.delete_timestamp is null and tt.bookkeeping_date between '2020-12-01' and '2020-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		-- clover Í¨¹ýÐÕÃûºÍbandwidthÄÜºÍspoonity Æ¥Åäµ½µÄÊý¾Ý £¬ÐèÒªÅÅ³ýÒÑ¾­ÊÇµÇÂ¼»áÔ±µ«ÓÖÊ¹ÓÃcloverµÄ½»Ò×£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
		union all
		select  s.id id
		from transactions tt with(index (trans_band_phone_idx))
			join shops s on s.id = tt.shop_id and (tt.spoonity_member is null or tt.spoonity_member='')
			join(
				select distinct phone,first_name from trans_spoonity_member
			) p1 on tt.bandwidth_phone is not null and tt.bandwidth_phone <> '' and ltrim(replace(replace(replace(tt.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
		where tt.delete_timestamp is null and tt.bookkeeping_date between '2020-12-01' and '2020-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	) p2 group by p2.id
) tsm on tsm.id = total.id
left join (
	select
    count(*) gfo_count, s.id id
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join shops s on s.id = t.shop_id
		left join
		(
			select distinct phone from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null and t.bookkeeping_date between '2020-12-01' and '2020-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		and p1.phone is null
		group by s.id
) gfo on gfo.id = tsm.id

/* All time visit count by date */
select sum(total.all_count) all_count, sum(COALESCE(tsm.tsm_count, 0)) tsm_count, sum(COALESCE(gfo.gfo_count, 0)) gfo_count, total.d d
from (
	select count(*) all_count, max(s.description) shop_name, s.id id, CAST(t.bookkeeping_date as date) d
	from transactions t
	join shops s on s.id = t.shop_id
	where t.bookkeeping_date between '2015-01-01' and '2030-11-30'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	group by s.id, CAST(t.bookkeeping_date as date)) total
left join (
	select count(*) tsm_count,id , CAST(p2.bookkeeping_date as date) d from
	(
		select  s.id id,tt.bookkeeping_date
		from transactions tt
			join trans_spoonity_member tsm on tt.id = tsm.transaction_id
			join shops s on s.id = tt.shop_id
		where tt.delete_timestamp is null and tt.bookkeeping_date between '2014-12-01' and '2030-12-15'
			and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		-- global food ÄÜÆ¥Åäµ½spoonityµÄÊý¾Ý£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
		union all
		select   s.id id,tt.bookkeeping_date
		from transactions tt
			join global_food_order gfo on tt.global_food_order_id = gfo.id
			join shops s on s.id = tt.shop_id
			join(
				select distinct phone from trans_spoonity_member
			) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
		where tt.delete_timestamp is null and tt.bookkeeping_date between '2014-12-01' and '2030-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		-- clover Í¨¹ýÐÕÃûºÍbandwidthÄÜºÍspoonity Æ¥Åäµ½µÄÊý¾Ý £¬ÐèÒªÅÅ³ýÒÑ¾­ÊÇµÇÂ¼»áÔ±µ«ÓÖÊ¹ÓÃcloverµÄ½»Ò×£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
		union all
		select  s.id id,tt.bookkeeping_date
		from transactions tt with(index (trans_band_phone_idx))
			join shops s on s.id = tt.shop_id and (tt.spoonity_member is null or tt.spoonity_member='')
			join(
				select distinct phone,first_name from trans_spoonity_member
			) p1 on tt.bandwidth_phone is not null and tt.bandwidth_phone <> '' and ltrim(replace(replace(replace(tt.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
		where tt.delete_timestamp is null and tt.bookkeeping_date between '2014-12-01' and '2030-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	) p2 group by p2.id,CAST(p2.bookkeeping_date as date)
) tsm on tsm.d = total.d and tsm.id = total.id
left join (
	select
    count(*) gfo_count, max(s.description) shop_name, s.id id, CAST(t.bookkeeping_date as date) d
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join shops s on s.id = t.shop_id
		left join
		(
			select distinct phone from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null and t.bookkeeping_date between '2014-12-01' and '2030-12-15'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
		and p1.phone is null
		group by s.id,CAST(t.bookkeeping_date as date)

) gfo on gfo.d = tsm.d and gfo.id = tsm.id
group by total.d
order by d

/* customer purchasing behavior */
select
max(clover_id) 'spoonity ID',
phone 'Phone number',
max(email) 'Email',
max(name) 'Name',
code 'Code',
max(description) 'Article name',
sum(qty) 'qty'
from
(
	select
		max(clover_id) clover_id,
		max(email) email,
		phone,
		max(name) name,
		code ,
		description ,
		sum(qty) qty
	from
	(
		 --ÒÔÏÂÊÇspoonityµÄÊý¾Ý
		select
		tsm.clover_id ,
		tsm.email ,
		tsm.phone,
		concat(tsm.first_name,'',tsm.last_name) name,
		a.code ,
		a.description ,
		ta.qty_weight qty
		 from trans_spoonity_member tsm
		join transactions t on tsm.transaction_id  = t.id
		join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
		join articles a on ta.article_id = a.id
		where t.delete_timestamp is null
		--group by tsm.clover_id,tsm.phone, tsm.email,tsm.first_name,tsm.last_name,a.id,a.description,a.code
		union all
		select
			p1.clover_id ,
			p1.email ,
			ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) phone,
			p1.name,
			a.code ,
			a.description ,
			ta.qty_weight qty
		from transactions t
			join global_food_order gfo on t.global_food_order_id = gfo.id
			join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
			join articles a on ta.article_id = a.id
			join(
				select distinct phone,clover_id,email,concat(first_name,' ',last_name) name from trans_spoonity_member
			) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
		where t.delete_timestamp is null
		-- clover Í¨¹ýÐÕÃûºÍbandwidthÄÜºÍspoonity Æ¥Åäµ½µÄÊý¾Ý £¬ÐèÒªÅÅ³ýÒÑ¾­ÊÇµÇÂ¼»áÔ±µ«ÓÖÊ¹ÓÃcloverµÄ½»Ò×£¬´ËÀàÊý¾ÝµÄ¿Í»§Ñ¶Ï¢È«²¿°´ÕÕspoonityµÄÊý¾Ý×ö¼ÇÂ¼£¬ÈçÃû³Æ£¬µç»°£¬emialÑ¶Ï¢µÈµÈ
		union all
		select
			p1.clover_id ,
			p1.email ,
			ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) phone,
			p1.name,
			a.code ,
			a.description ,
			ta.qty_weight qty
		from transactions t with(index (trans_band_phone_idx))
			join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
			join articles a on ta.article_id = a.id
			join(
				select distinct phone, clover_id,email,concat(first_name,' ',last_name) name from trans_spoonity_member
			) p1 on t.bandwidth_phone is not null and t.bandwidth_phone <> '' and ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
		where t.delete_timestamp is null
	) T1 group by T1.phone,T1.clover_id,T1.code,T1.description

	UNION ALL
	  --ÒÔÏÂÊÇGFµÄÊý¾Ý
	select
		null clover_id,
		max(gfo.client_email) email,
		ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) phone,
		concat(max(gfo.client_first_name),'',max(gfo.client_last_name)) name,
		a.code ,
		a.description ,
		sum(ta.qty_weight) qty
	from transactions t
		join global_food_order gfo on t.global_food_order_id = gfo.id
		join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
		join articles a on ta.article_id = a.id
		left join
		(
			select distinct phone from trans_spoonity_member
		) p1 on ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null and p1.phone is null
	group by gfo.client_phone,a.id,a.code,a.description
) t
group by t.phone,t.code
order by max(qty) desc, max(clover_id) desc, max(email) desc
OFFSET 0 rows
FETCH NEXT 10 ROWS ONLY

/* Best selling items */
select
t.description,
sum(qty) 'qty'
from
(
	 --ÒÔÏÂÊÇspoonityµÄÊý¾Ý
	select
	a.description,
	sum(ta.qty_weight) qty
	 from trans_spoonity_member tsm
	join transactions t on tsm.transaction_id  = t.id
	join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
	join articles a on ta.article_id = a.id
	join shops s on s.id = t.shop_id
	where t.delete_timestamp is null
		and t.bookkeeping_date = '2020-12-14'
		and a.article_type = '1'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	group by a.description
	UNION ALL
	select
		a.description,
		sum(ta.qty_weight) qty
	from transactions t with(index (trans_band_phone_idx))
		join shops s on s.id = t.shop_id and (t.spoonity_member is null or t.spoonity_member='')
		join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
		join articles a on ta.article_id = a.id
		join(
			select distinct phone,first_name from trans_spoonity_member
		) p1 on t.bandwidth_phone is not null and t.bandwidth_phone <> '' and ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
	where t.delete_timestamp is null
	and t.bookkeeping_date = '2020-12-14'
	and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	group by a.description
	UNION ALL
	  --ÒÔÏÂÊÇGFµÄÊý¾Ý
	select
	a.description,
	sum(ta.qty_weight) qty
	from transactions t
	join global_food_order gfo on t.global_food_order_id = gfo.id
	join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
	join articles a on ta.article_id = a.id
	join shops s on s.id = t.shop_id
	where t.delete_timestamp is null
		and t.bookkeeping_date = '2020-12-14'
		and a.article_type = '1'
		and s.description in ('TEMPLE', 'IRVINE', 'CUPERTINO')
	group by a.description
) t
group by t.description
order by max(qty) desc

/*It's not the member's cell phone number yet*/
select
	ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) phone,
	max(card_number) card_number,
    max(bookkeeping_date) last_visit,
	min(bookkeeping_date) first_visit,
	datediff(day, max(bookkeeping_date), GETDATE()) last_visit_days_ago,
	ISNULL(count(*) * 1.0 / NULLIF(datediff(week, min(bookkeeping_date), max(bookkeeping_date)), 0), count(*)) visit_ratio,
    max(name) name,
    count(*) visit_count
from transactions t with(index (trans_band_phone_idx))
join(
	select transaction_id, max(Clover_Card_Hold_Name) name,max(credit_card_authorization_num) card_number
	 from trans_payments
	 group by transaction_id
) tp on t.id=tp.transaction_id
left join (
	select distinct phone from trans_spoonity_member
) p1 on ltrim(replace(replace(replace(t.bandwidth_phone,'+1',''),'-',''),'+ 1','')) = p1.phone
where p1.phone is null and t.bandwidth_phone is not null and t.bandwidth_phone <> ''
group by  t.bandwidth_phone
order by count(*) desc
