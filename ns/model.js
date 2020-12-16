const sql = require("mssql")
require('dotenv').config()

const CONFIG = {
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  server: process.env.DB_HOST,
  database: 'meetfresh'
}
exports.getCustomerVisitsCount = async function(period, shop) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .input('from', sql.VarChar, period.from)
      .input('to', sql.VarChar, period.to)
      .query(`
        select total.shop_name, total.all_count, tsm.tsm_count, gfo.gfo_count
          from (
        	select count(*) all_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	where t.bookkeeping_date between @from and @to
            ${shop_filter}
        	group by s.id) total
        left join (
        	select count(*) tsm_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join trans_spoonity_member tsm on t.id = tsm.transaction_id
        	where t.bookkeeping_date between @from and @to
            ${shop_filter}
        	group by s.id
        ) tsm on tsm.id = total.id
        left join (
        	select count(*) gfo_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join global_food_order gfo on t.global_food_order_id = gfo.id
        	where t.bookkeeping_date between @from and @to
            ${shop_filter}
        	group by s.id
        ) gfo on gfo.id = tsm.id
      `)
    return result
  }catch(err){
    return err
  }
}
exports.getCustomerVisitsCountByDate = async function(period, shop) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .input('from', sql.VarChar, period.from)
      .input('to', sql.VarChar, period.to)
      .query(`
        select sum(total.all_count) all_count, sum(COALESCE(tsm.tsm_count, 0)) tsm_count, sum(COALESCE(gfo.gfo_count, 0)) gfo_count, total.d d
        from (
        	select count(*) all_count, max(s.description) shop_name, s.id id, CAST(t.bookkeeping_date as date) d
        	from transactions t
        	join shops s on s.id = t.shop_id
        	where t.bookkeeping_date between @from and @to
        		${shop_filter}
        	group by s.id, CAST(t.bookkeeping_date as date)) total
        left join (
        	select count(*) tsm_count, max(s.description) shop_name, s.id id, CAST(t.bookkeeping_date as date) d
        	from transactions t 
        	join shops s on s.id = t.shop_id
        	join trans_spoonity_member tsm on t.id = tsm.transaction_id
        	where t.bookkeeping_date between @from and @to
        		${shop_filter}
        	group by s.id, CAST(t.bookkeeping_date as date)
        ) tsm on tsm.d = total.d and tsm.id = total.id
        left join (
        	select count(*) gfo_count, max(s.description) shop_name, s.id id, CAST(t.bookkeeping_date as date) d
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join global_food_order gfo on t.global_food_order_id = gfo.id
        	where t.bookkeeping_date between @from and @to
        		${shop_filter}
        	group by s.id, CAST(t.bookkeeping_date as date)
        ) gfo on gfo.d = tsm.d and gfo.id = tsm.id
        group by total.d
        order by d
      `)
    return result
  }catch(err){
    return err
  }
}
exports.getCustomerVisitsCountCompare = async function(period, shop) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .input('from', sql.VarChar, period.from)
      .input('to', sql.VarChar, period.to)
      .query(`
        select total.shop_name, total.all_count, tsm.tsm_count, gfo.gfo_count
          from (
        	select count(*) all_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	where t.bookkeeping_date between @from and @to
          ${shop_filter}
        	group by s.id) total
        left join (
        	select count(*) tsm_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join trans_spoonity_member tsm on t.id = tsm.transaction_id
        	where t.bookkeeping_date between @from and @to
          ${shop_filter}
        	group by s.id
        ) tsm on tsm.id = total.id
        left join (
        	select count(*) gfo_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join global_food_order gfo on t.global_food_order_id = gfo.id
        	where t.bookkeeping_date between @from and @to
          ${shop_filter}
        	group by s.id
        ) gfo on gfo.id = tsm.id
      `)
    return result
  }catch(err){
    return err
  }
}
exports.getCustomerVisitInfo = async function(offset, limit, customer_type, shop, search_key) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  let all_query = `
    select
      tsm.clover_id spoonity_id,
    null gfo_id,
      tsm.email email,
      tsm.phone phone,
    max(tsm.birth_day) birthday,
    ISNULL(datediff(day, GETDATE(), max(tsm.birth_day)), -999) days_remaining_until_birthday,
      max(bookkeeping_date) last_visit,
    min(bookkeeping_date) first_visit,
    datediff(day, max(bookkeeping_date), GETDATE()) last_visit_days_ago,
    ISNULL(count(*) * 1.0 / NULLIF(datediff(week, min(bookkeeping_date), max(bookkeeping_date)), 0), count(*)) visit_ratio,
      concat(max(tsm.first_name),' ',max(tsm.last_name)) name,
      count(*) visit_count
    from transactions t
        join trans_spoonity_member tsm on t.id = tsm.transaction_id
      join shops s on s.id = t.shop_id
    where t.delete_timestamp is null
      ${shop_filter}
    group by tsm.clover_id,tsm.phone,tsm.email
    union all
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
    where t.delete_timestamp is null
      ${shop_filter}
    group by ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')), gfo.client_email
    order by visit_count desc, max(bookkeeping_date) desc
    OFFSET @offset ROWS
    FETCH NEXT @limit ROWS ONLY
  `
  let spoonity_query = `
    select
      tsm.clover_id spoonity_id,
    null gfo_id,
      tsm.email email,
      tsm.phone phone,
    max(tsm.birth_day) birthday,
    ISNULL(datediff(day, GETDATE(), max(tsm.birth_day)), -999) days_remaining_until_birthday,
      max(bookkeeping_date) last_visit,
    min(bookkeeping_date) first_visit,
    datediff(day, max(bookkeeping_date), GETDATE()) last_visit_days_ago,
    ISNULL(count(*) * 1.0 / NULLIF(datediff(week, min(bookkeeping_date), max(bookkeeping_date)), 0), count(*)) visit_ratio,
      concat(max(tsm.first_name),' ',max(tsm.last_name)) name,
      count(*) visit_count
    from transactions t
        join trans_spoonity_member tsm on t.id = tsm.transaction_id
      join shops s on s.id = t.shop_id
    where t.delete_timestamp is null
      ${shop_filter}
    group by tsm.clover_id,tsm.phone,tsm.email
    order by visit_count desc, max(bookkeeping_date) desc
    OFFSET @offset ROWS
    FETCH NEXT @limit ROWS ONLY
  `
  let gfo_query = `
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
    where t.delete_timestamp is null
      ${shop_filter}
    group by ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')), gfo.client_email
    order by visit_count desc, max(bookkeeping_date) desc
    OFFSET @offset ROWS
    FETCH NEXT @limit ROWS ONLY
  `
  let run_query = ''
  if(customer_type == 'all'){
    run_query = all_query
  }else if(customer_type == 'spoonity'){
    run_query = spoonity_query
  }else{
    run_query = gfo_query
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .input('offset', sql.Int, offset)
      .input('limit', sql.Int, limit)
      // .input('search_key', sql.String, search_key)
      .query(run_query)
    return result
  }catch(err){
    return err
  }
}
exports.getAllTimeVisitCount = async function(shop) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .query(`
        select total.shop_name, total.all_count, tsm.tsm_count, gfo.gfo_count
        from (
        	select count(*) all_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	where t.delete_timestamp is null
        		and t.bookkeeping_date between '2015-12-01' and '2020-12-15'
        		${shop_filter}
        	group by s.id) total
        left join (
        	select count(*) tsm_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join trans_spoonity_member tsm on t.id = tsm.transaction_id
        	where t.delete_timestamp is null
        		and t.bookkeeping_date between '2015-12-01' and '2020-12-15'
        		${shop_filter}
        	group by s.id
        ) tsm on tsm.id = total.id
        left join (
        	select count(*) gfo_count, max(s.description) shop_name, s.id id
        	from transactions t
        	join shops s on s.id = t.shop_id
        	join global_food_order gfo on t.global_food_order_id = gfo.id
        	where t.delete_timestamp is null
        		and t.bookkeeping_date between '2015-12-01' and '2020-12-15'
        		${shop_filter}
        	group by s.id
        ) gfo on gfo.id = tsm.id
      `)
    return result
  }catch(err){
    return err
  }
}
exports.getAllCustomersCount = async function(shop) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .query(`
        select 'tsm_customers_count' customer_type, count(*) customer_count
        from (select
            1 as something
        from transactions t
            join trans_spoonity_member tsm on t.id = tsm.transaction_id
        	join shops s on s.id = t.shop_id
        where t.delete_timestamp is null
        	${shop_filter}
        group by tsm.clover_id,tsm.phone,tsm.email) tsm_customers
        union all
        select 'gfo_customers_count' customer_type, count(*) customer_count
        from (select
            1 as something
        from transactions t
            join global_food_order gfo on t.global_food_order_id = gfo.id
        	join shops s on s.id = t.shop_id
        where t.delete_timestamp is null
        	${shop_filter}
        group by ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')), gfo.client_email) gfo_customers
      `)
    return result
  }catch(err){
    return err
  }
}
exports.getTopSaleItems = async function(shop, period, offset, limit) {
  let shop_filter = ''
  if(shop.length === 1){
    shop_filter = `and s.description = '${shop[0]}'` // Single shop filter
  }else{
    let shops = ``;
    shop.forEach(item => {
      shops += `'${item}',`
    })
    shops = shops.slice(0, -1) // Remove tailing ,
    shop_filter = `and s.description in (${shops})` // Multi shop filter
  }
  try{
    let pool = await sql.connect(CONFIG)
    let result = await pool.request()
      .input('from', sql.VarChar, period.from)
      .input('to', sql.VarChar, period.to)
      .input('offset', sql.Int, offset)
      .input('limit', sql.Int, limit)
      .query(`
        select
        t.description,
        sum(qty) 'qty',
        sum(price) 'price'
        from
        (
        	select
        	a.description,
        	sum(ta.qty_weight) qty,
          sum(ta.price) price
        	 from trans_spoonity_member tsm
        	join transactions t on tsm.transaction_id  = t.id
        	join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
        	join articles a on ta.article_id = a.id
        	join shops s on s.id = t.shop_id
        	where t.delete_timestamp is null
        		and t.bookkeeping_date between @from and @to
        		and a.article_type = '1'
        		${shop_filter}
        	group by a.description
        	UNION ALL
        	select
        	a.description,
        	sum(ta.qty_weight) qty,
          sum(ta.price) price
        	from transactions t
        	join global_food_order gfo on t.global_food_order_id = gfo.id
        	join trans_articles ta on t.id = ta.transaction_id and ta.delete_timestamp is null
        	join articles a on ta.article_id = a.id
        	join shops s on s.id = t.shop_id
        	where t.delete_timestamp is null
        		and t.bookkeeping_date between @from and @to
        		and a.article_type = '1'
        		${shop_filter}
        	group by a.description
        ) t
        group by t.description
        order by max(qty) desc
        OFFSET @offset ROWS
        FETCH NEXT @limit ROWS ONLY
      `)
    return result
  }catch(err){
    return err
  }
}
