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
exports.getCustomerVisitInfo = async function(offset, limit, period, shop, search_key) {
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
      .input('offset', sql.Int, offset)
      .input('limit', sql.Int, limit)
      // .input('search_key', sql.String, search_key)
      .input('from', sql.VarChar, period.from)
      .input('to', sql.VarChar, period.to)
      .query(`
        select
          tsm.clover_id spoonity_id,
      	  null gfo_id,
          tsm.email email,
          tsm.phone phone,
          max(bookkeeping_date) last_visit,
          concat(max(tsm.first_name),' ',max(tsm.last_name)) name,
          count(*) visit_count,
          max(s.description) shop_name
        from transactions t
          join trans_spoonity_member tsm on t.id = tsm.transaction_id
          join shops s on s.id = t.shop_id
        where t.delete_timestamp is null and (select count(price) from trans_articles where trans_articles.transaction_id = t.id and trans_articles.delete_timestamp is null) >0
          and t.bookkeeping_date between @from and @to
          ${shop_filter}
        group by tsm.clover_id,tsm.phone,tsm.email
        union all
        select
          null spoonity_id,
    	    max(gfo.id) gfo_id,
          gfo.client_email email,
      	  ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')) phone,
          max(bookkeeping_date) last_visit,
          concat(max(gfo.client_first_name),' ',max(gfo.client_last_name)) name,
          count(*) visit_count,
          max(s.description) shop_name
        from transactions t
          join global_food_order gfo on t.global_food_order_id = gfo.id
          join shops s on s.id = t.shop_id
        where t.delete_timestamp is null and (select count(price) from trans_articles where trans_articles.transaction_id = t.id and trans_articles.delete_timestamp is null) >0
          and t.bookkeeping_date between @from and @to
          ${shop_filter}
        group by ltrim(replace(replace(replace(gfo.client_phone,'+1',''),'-',''),'+ 1','')), gfo.client_email
        order by visit_count desc, max(bookkeeping_date) desc
        OFFSET @offset ROWS
        FETCH NEXT @limit ROWS ONLY
      `)
    return result
  }catch(err){
    return err
  }
}
