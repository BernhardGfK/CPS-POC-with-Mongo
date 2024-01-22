with trips as (
select household_id, pur_date, shop_id, 1 as trips
from purchases
group by household_id, pur_date, shop_id
)
select count(*) trip_unwgt, sum(w.weight*trips) trip_fs_wgt
from weights w, trips t
where t.pur_date between w.from_date and w.to_date
and w.household_id=t.household_id
and w.standard=1;

with trips as (
select household_id, pur_date, shop_id, avg(rw) as trips
from purchases
group by household_id, pur_date, shop_id
)
select count(*) trip_unwgt, sum(w.weight*trips) trip_rw
from weights w, trips t
where t.pur_date between w.from_date and w.to_date
and w.household_id=t.household_id
and w.standard=1;



with avg_weights as (
select household_id, sum(weight)/(select (julianday(max(to_date))-julianday(min(from_date))+1)/7 as weeks from weights where standard=1) as avg_weight
from weights
where standard=1
group by household_id),
trips as (
select household_id, pur_date, shop_id, 1 as trips
from purchases
group by household_id, pur_date, shop_id
)
select count(*) trip_unwgt, sum(avg_weight*trips) trip_avg
from avg_weights w, trips t
where w.household_id=t.household_id;
