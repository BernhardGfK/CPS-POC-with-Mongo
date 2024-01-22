with cswgt as (
select from_date, to_date
from weights
where from_date <='2017-01-01'
and to_date >='2018-12-31'
group by from_date, to_date
having to_date-from_date=min(to_date-from_date)
order by from_date
limit 1),
buyers as (
select household_id, 1 as buyers
from purchases
group by household_id
)
select count(*) buyers_unwgt, sum(w.weight*buyers) buyers_cs_wgt
from weights w, buyers t, cswgt cw
where w.household_id=t.household_id
and cw.from_date=w.from_date
and cw.to_date=w.to_date;

with avg_weights as (
select household_id, sum(weight)/(select (julianday(max(to_date))-julianday(min(from_date))+1)/7 as weeks from weights where standard=1) as avg_weight
from weights
where standard=1
group by household_id),
buyers as (
select household_id, 1 as buyers
from purchases
group by household_id)
select count(*) buyer_unwgt, sum(avg_weight*buyers) buyer_avg
from avg_weights w, buyers t
where w.household_id=t.household_id;
