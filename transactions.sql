select count(*) tran_unwgt, sum(w.weight*brand_factor) tran_wgt
from weights w, purchases p
where p.pur_date between w.from_date and w.to_date
and w.household_id=p.household_id
and w.standard=1;

select aa.postext, count(*) tran_unwgt, sum(w.weight*brand_factor) tran_wgt
from weights w, purchases p, artaxis aa
where p.pur_date between w.from_date and w.to_date
and w.household_id=p.household_id
and aa.cat_id=p.category_id
and aa.brnd_id=p.brand_id
and w.standard=1
group by aa.postext;

with hh as (
select household_id, bdl_id, age_id
from households
group by household_id
having valid_date=max(valid_date)
)
select h.postext, count(*) tran_unwgt, sum(w.weight*brand_factor) tran_wgt
from weights w, purchases p, hhaxis h, hh
where p.pur_date between w.from_date and w.to_date
and w.household_id=p.household_id
and hh.household_id=p.household_id
and h.age_id=hh.age_id
and h.bdl_id=hh.bdl_id
and w.standard=1
group by h.postext;

with hh as (
select household_id, bdl_id, age_id
from households
group by household_id
having valid_date=max(valid_date)
)
select h.postext, aa.postext, count(*) tran_unwgt, sum(w.weight*brand_factor) tran_wgt
from weights w, purchases p, hhaxis h, hh, artaxis aa
where p.pur_date between w.from_date and w.to_date
and w.household_id=p.household_id
and hh.household_id=p.household_id
and h.age_id=hh.age_id
and h.bdl_id=hh.bdl_id
and aa.cat_id=p.category_id
and aa.brnd_id=p.brand_id
and w.standard=1
group by h.postext, aa.postext;
